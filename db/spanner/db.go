// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package spanner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	mexporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	spannerDBName             = "spanner.db"
	spannerCredentials        = "spanner.credentials"
	otelProjectIDEnv          = "OTEL_PROJECT_ID"
	otelMetricProjectIDEnv    = "OTEL_METRIC_PROJECT_ID"
	otelServiceNameEnv        = "OTEL_SERVICE_NAME"
	otelExportIntervalEnv     = "OTEL_METRIC_EXPORT_INTERVAL_SECONDS"
	defaultOTELServiceName    = "go-ycsb-spanner"
	defaultOTELExportInterval = 10 * time.Second
)

type spannerCreator struct {
}

type spannerDB struct {
	p              *properties.Properties
	client         *spanner.Client
	verbose        bool
	shutdownMetric func(context.Context) error
}

var newGoogleCloudMetricExporter = func(projectID string) (sdkmetric.Exporter, error) {
	return mexporter.New(mexporter.WithProjectID(projectID))
}

type contextKey string

const stateKey = contextKey("spannerDB")

type spannerState struct {
}

type rpcError struct {
	op          string
	details     string
	statusFound bool
	code        codes.Code
	message     string
	err         error
}

func (e *rpcError) Error() string {
	if !e.statusFound {
		return fmt.Sprintf("spanner %s failed (%s): %v", e.op, e.details, e.err)
	}
	if e.code == codes.OK {
		return fmt.Sprintf("spanner %s failed (%s): %s", e.op, e.details, e.message)
	}
	return fmt.Sprintf("spanner %s failed (%s): code=%s message=%q err=%v", e.op, e.details, e.code, e.message, e.err)
}

func (e *rpcError) Unwrap() error {
	return e.err
}

func sanitizeMetricToken(s string) string {
	var b strings.Builder
	lastUnderscore := false
	prevWasLowerOrDigit := false

	for _, r := range s {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			if unicode.IsUpper(r) && prevWasLowerOrDigit && !lastUnderscore && b.Len() > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(unicode.ToUpper(r))
			lastUnderscore = false
			prevWasLowerOrDigit = unicode.IsLower(r) || unicode.IsDigit(r)
		case !lastUnderscore && b.Len() > 0:
			b.WriteByte('_')
			lastUnderscore = true
			prevWasLowerOrDigit = false
		default:
			prevWasLowerOrDigit = false
		}
	}

	token := strings.Trim(b.String(), "_")
	if token == "" {
		return "UNKNOWN"
	}

	runes := []rune(token)
	if len(runes) > 64 {
		token = strings.Trim(string(runes[:64]), "_")
	}
	if token == "" {
		return "UNKNOWN"
	}
	return token
}

func (e *rpcError) metricName(op string) string {
	parts := []string{
		sanitizeMetricToken(op),
		"ERROR",
		"SPANNER",
		sanitizeMetricToken(e.op),
	}

	if e.statusFound {
		parts = append(parts, sanitizeMetricToken(e.code.String()), sanitizeMetricToken(e.message))
	} else {
		parts = append(parts, "NON_GRPC", sanitizeMetricToken(e.err.Error()))
	}

	return strings.Join(parts, "_")
}

func formatRPCError(op string, err error, details string) error {
	if err == nil {
		return nil
	}

	rpcErr := &rpcError{
		op:      op,
		details: details,
		err:     err,
	}

	st, ok := status.FromError(err)
	if !ok {
		return rpcErr
	}

	rpcErr.statusFound = true
	rpcErr.code = st.Code()
	rpcErr.message = st.Message()
	return rpcErr
}

func (db *spannerDB) ErrorMetricName(op string, err error) string {
	if op != "READ" {
		return ""
	}

	var rpcErr *rpcError
	if !errors.As(err, &rpcErr) {
		return ""
	}

	return rpcErr.metricName(op)
}

func firstNonEmptyEnv(keys ...string) string {
	for _, key := range keys {
		value := strings.TrimSpace(os.Getenv(key))
		if value != "" {
			return value
		}
	}
	return ""
}

func openTelemetryMetricSettingsFromEnv() (projectID string, serviceName string, interval time.Duration, enabled bool, err error) {
	projectID = firstNonEmptyEnv(otelProjectIDEnv, otelMetricProjectIDEnv)
	if projectID == "" {
		return "", "", 0, false, nil
	}

	serviceName = firstNonEmptyEnv(otelServiceNameEnv)
	if serviceName == "" {
		serviceName = defaultOTELServiceName
	}

	interval = defaultOTELExportInterval
	if raw := strings.TrimSpace(os.Getenv(otelExportIntervalEnv)); raw != "" {
		seconds, convErr := strconv.Atoi(raw)
		if convErr != nil || seconds <= 0 {
			return "", "", 0, false, fmt.Errorf("invalid %s=%q: must be a positive integer", otelExportIntervalEnv, raw)
		}
		interval = time.Duration(seconds) * time.Second
	}

	return projectID, serviceName, interval, true, nil
}

func configureOpenTelemetryMetrics(cfg *spanner.ClientConfig) (func(context.Context) error, error) {
	// Only export metrics when OTEL_PROJECT_ID is set. Keep Spanner's native exporter
	// disabled so go-ycsb does not emit client metrics implicitly.
	cfg.DisableNativeMetrics = true

	projectID, serviceName, exportInterval, enabled, err := openTelemetryMetricSettingsFromEnv()
	if err != nil {
		return nil, err
	}
	if !enabled {
		return nil, nil
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, err
	}

	exporter, err := newGoogleCloudMetricExporter(projectID)
	if err != nil {
		return nil, err
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(exportInterval)),
		),
	)

	cfg.OpenTelemetryMeterProvider = meterProvider
	cfg.ExportBuiltInMetricsToOpenTelemetry = true

	return meterProvider.Shutdown, nil
}

func (c spannerCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := new(spannerDB)
	d.p = p

	ctx := context.Background()

	dbName := p.GetString(spannerDBName, "")
	if len(dbName) == 0 {
		return nil, fmt.Errorf("must provide a database like projects/xxxx/instances/xxxx/databases/xxx")
	}
	// client, err := spanner.NewClient(ctx, dbName)

	endpoint := os.Getenv("ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:15000"
	}

	opts := []option.ClientOption{
		option.WithEndpoint(endpoint),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}

	cfg := spanner.ClientConfig{
		IsExperimentalHost: true,
	}
	shutdownMetric, err := configureOpenTelemetryMetrics(&cfg)
	if err != nil {
		return nil, err
	}

	if chStr := os.Getenv("SPANNER_NUM_CHANNELS"); chStr != "" {
		if ch, err := strconv.Atoi(chStr); err == nil && ch > 0 {
			cfg.NumChannels = ch
		}
	}

	client, err := spanner.NewClientWithConfig(ctx, dbName, cfg, opts...)
	if err != nil {
		if shutdownMetric != nil {
			_ = shutdownMetric(ctx)
		}
		return nil, err
	}
	d.client = client
	d.shutdownMetric = shutdownMetric
	return d, nil
}

func (db *spannerDB) createDatabase(ctx context.Context, adminClient *database.DatabaseAdminClient, dbName string) (string, error) {
	matches := regexp.MustCompile("^(.*)/databases/(.*)$").FindStringSubmatch(dbName)
	if matches == nil || len(matches) != 3 {
		return "", fmt.Errorf("Invalid database id %s", dbName)
	}

	database, err := adminClient.GetDatabase(ctx, &adminpb.GetDatabaseRequest{
		Name: dbName,
	})
	if err != nil {
		return "", err
	}

	if database.State == adminpb.Database_STATE_UNSPECIFIED {
		op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
			Parent:          matches[1],
			CreateStatement: "CREATE DATABASE `" + matches[2] + "`",
			ExtraStatements: []string{},
		})
		if err != nil {
			return "", err
		}
		if _, err := op.Wait(ctx); err != nil {
			return "", err
		}
	}

	return matches[2], nil
}

func (db *spannerDB) tableExisted(ctx context.Context, table string) (bool, error) {
	stmt := spanner.NewStatement(`SELECT t.table_name FROM information_schema.tables AS t 
	WHERE t.table_catalog = '' AND t.table_schema = '' AND t.table_name = @name`)
	stmt.Params["name"] = table
	iter := db.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	found := false
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return false, err
		}
		found = true
		break
	}

	return found, nil
}

func (db *spannerDB) createTable(ctx context.Context, adminClient *database.DatabaseAdminClient, dbName string) error {
	tableName := db.p.GetString(prop.TableName, prop.TableNameDefault)
	fieldCount := db.p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	fieldLength := db.p.GetInt64(prop.FieldLength, prop.FieldLengthDefault)

	existed, err := db.tableExisted(ctx, tableName)
	if err != nil {
		return err
	}

	if db.p.GetBool(prop.DropData, prop.DropDataDefault) && existed {
		op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
			Database: dbName,
			Statements: []string{
				fmt.Sprintf("DROP TABLE %s", tableName),
			},
		})
		if err != nil {
			return err
		}

		if err = op.Wait(ctx); err != nil {
			return err
		}
		existed = false
	}

	if existed {
		return nil
	}

	buf := new(bytes.Buffer)
	s := fmt.Sprintf("CREATE TABLE  %s (id STRING(%d)", tableName, fieldLength)
	buf.WriteString(s)

	for i := int64(0); i < fieldCount; i++ {
		buf.WriteString(fmt.Sprintf(", field%d STRING(%d)", i, fieldLength))
	}

	buf.WriteString(") PRIMARY KEY (id)")

	op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database: dbName,
		Statements: []string{
			buf.String(),
		},
	})
	if err != nil {
		return err
	}

	if err := op.Wait(ctx); err != nil {
		return err
	}

	return nil
}

func (db *spannerDB) Close() error {
	var err error
	if db.client != nil {
		db.client.Close()
	}
	if db.shutdownMetric != nil {
		err = errors.Join(err, db.shutdownMetric(context.Background()))
	}
	return err
}

func (db *spannerDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	state := &spannerState{}

	return context.WithValue(ctx, stateKey, state)
}

func (db *spannerDB) CleanupThread(ctx context.Context) {
	//	state := ctx.Value(stateKey).(*spanner)
}

func (db *spannerDB) queryRows(ctx context.Context, stmt spanner.Statement, count int) ([]map[string][]byte, error) {
	if db.verbose {
		fmt.Printf("%s %v\n", stmt.SQL, stmt.Params)
	}

	iter := db.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	vs := make([]map[string][]byte, 0, count)
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, formatRPCError("query next", err, fmt.Sprintf("sql=%q params=%v", stmt.SQL, stmt.Params))
		}

		rowSize := row.Size()
		m := make(map[string][]byte, rowSize)
		dest := make([]interface{}, rowSize)
		for i := 0; i < rowSize; i++ {
			v := new(spanner.NullString)
			dest[i] = v
		}

		if err := row.Columns(dest...); err != nil {
			return nil, formatRPCError("query decode", err, fmt.Sprintf("sql=%q params=%v", stmt.SQL, stmt.Params))
		}

		for i := 0; i < rowSize; i++ {
			v := dest[i].(*spanner.NullString)
			if v.Valid {
				m[row.ColumnName(i)] = util.Slice(v.StringVal)
			}
		}

		vs = append(vs, m)
	}

	return vs, nil
}

func (db *spannerDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	if len(fields) == 0 {
		fieldCount := db.p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
		fields = make([]string, 0, 1+fieldCount)
		fields = append(fields, "id")
		for i := int64(0); i < fieldCount; i++ {
			fields = append(fields, fmt.Sprintf("field%d", i))
		}
	}

	keySet := spanner.Key{key}
	iter := db.client.Single().Read(ctx, table, keySet, fields)
	defer iter.Stop()

	row, err := iter.Next()
	if err == iterator.Done {
		return nil, nil
	}
	if err != nil {
		return nil, formatRPCError("read next", err, fmt.Sprintf("table=%s key=%q fields=%v", table, key, fields))
	}

	rowSize := row.Size()
	m := make(map[string][]byte, rowSize)
	dest := make([]interface{}, rowSize)
	for i := 0; i < rowSize; i++ {
		dest[i] = new(spanner.NullString)
	}

	if err := row.Columns(dest...); err != nil {
		return nil, formatRPCError("read decode", err, fmt.Sprintf("table=%s key=%q fields=%v", table, key, fields))
	}

	for i := 0; i < rowSize; i++ {
		v := dest[i].(*spanner.NullString)
		if v.Valid {
			m[row.ColumnName(i)] = util.Slice(v.StringVal)
		}
	}

	return m, nil
}

func (db *spannerDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf(`SELECT * FROM %s WHERE id >= @key LIMIT @limit`, table)
	} else {
		query = fmt.Sprintf(`SELECT %s FROM %s WHERE id >= @key LIMIT @limit`, strings.Join(fields, ","), table)
	}

	stmt := spanner.NewStatement(query)
	stmt.Params["key"] = startKey
	stmt.Params["limit"] = count

	rows, err := db.queryRows(ctx, stmt, count)

	return rows, err
}

func createMutations(key string, mutations map[string][]byte) ([]string, []interface{}) {
	keys := make([]string, 0, 1+len(mutations))
	values := make([]interface{}, 0, 1+len(mutations))
	keys = append(keys, "id")
	values = append(values, key)

	for key, value := range mutations {
		keys = append(keys, key)
		values = append(values, util.String(value))
	}

	return keys, values
}

func (db *spannerDB) Update(ctx context.Context, table string, key string, mutations map[string][]byte) error {
	keys, values := createMutations(key, mutations)
	m := spanner.Update(table, keys, values)
	_, err := db.client.Apply(ctx, []*spanner.Mutation{m})
	return formatRPCError("update", err, fmt.Sprintf("table=%s key=%q columns=%v mutation_fields=%d", table, key, keys, len(mutations)))
}

func (db *spannerDB) Insert(ctx context.Context, table string, key string, mutations map[string][]byte) error {
	keys, values := createMutations(key, mutations)
	m := spanner.InsertOrUpdate(table, keys, values)
	_, err := db.client.Apply(ctx, []*spanner.Mutation{m})
	return formatRPCError("insert", err, fmt.Sprintf("table=%s key=%q columns=%v mutation_fields=%d", table, key, keys, len(mutations)))
}

func (db *spannerDB) Delete(ctx context.Context, table string, key string) error {
	m := spanner.Delete(table, spanner.Key{key})
	_, err := db.client.Apply(ctx, []*spanner.Mutation{m})
	return formatRPCError("delete", err, fmt.Sprintf("table=%s key=%q", table, key))
}

func init() {
	ycsb.RegisterDBCreator("spanner", spannerCreator{})
}
