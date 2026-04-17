package main

import "testing"

func TestParseRuntimeProfileConfigFromEnvDefaults(t *testing.T) {
	cfg, err := parseRuntimeProfileConfigFromEnv()
	if err != nil {
		t.Fatalf("parseRuntimeProfileConfigFromEnv returned error: %v", err)
	}
	if cfg.mutexFraction != 0 {
		t.Fatalf("mutexFraction = %d, want 0", cfg.mutexFraction)
	}
	if cfg.blockRate != 0 {
		t.Fatalf("blockRate = %d, want 0", cfg.blockRate)
	}
}

func TestParseRuntimeProfileConfigFromEnvConfigured(t *testing.T) {
	t.Setenv(pprofMutexFractionEnv, "10")
	t.Setenv(pprofBlockRateEnv, "1")

	cfg, err := parseRuntimeProfileConfigFromEnv()
	if err != nil {
		t.Fatalf("parseRuntimeProfileConfigFromEnv returned error: %v", err)
	}
	if cfg.mutexFraction != 10 {
		t.Fatalf("mutexFraction = %d, want 10", cfg.mutexFraction)
	}
	if cfg.blockRate != 1 {
		t.Fatalf("blockRate = %d, want 1", cfg.blockRate)
	}
}

func TestParseRuntimeProfileConfigFromEnvRejectsInvalidValues(t *testing.T) {
	t.Setenv(pprofMutexFractionEnv, "-1")
	if _, err := parseRuntimeProfileConfigFromEnv(); err == nil {
		t.Fatal("expected invalid mutex fraction to fail")
	}

	t.Setenv(pprofMutexFractionEnv, "")
	t.Setenv(pprofBlockRateEnv, "abc")
	if _, err := parseRuntimeProfileConfigFromEnv(); err == nil {
		t.Fatal("expected invalid block rate to fail")
	}
}
