package main

import (
	"encoding/json"
	"fmt"

	"github.com/codefly-dev/core/resources"
	"github.com/codefly-dev/service-object-storage/internal/sessions"
)

// ScopedGrant is one base the service's consumer may ask scoped sessions
// for, with the modes it may use there (read, ingest, maintenance) and,
// optionally, the only partitions it may name.
type ScopedGrant struct {
	Base       string   `yaml:"base" json:"base"`
	Modes      []string `yaml:"modes" json:"modes"`
	Partitions []string `yaml:"partitions" json:"partitions,omitempty"`
}

// scopedEnvironment is what the gateway is told about scoped sessions. With
// the setting unset or "off" it is told nothing, and behaves as it always
// did. The grants are validated here, by the gateway's own parser, so a spec
// the gateway would refuse fails the run before a container starts.
func scopedEnvironment(settings *Settings) ([]*resources.EnvironmentVariable, error) {
	if settings == nil || settings.ScopedSessions == "" || settings.ScopedSessions == "off" {
		return nil, nil
	}
	if settings.ScopedSessions != "allow" && settings.ScopedSessions != "require" {
		return nil, fmt.Errorf("scoped-sessions is %q: want off, allow or require", settings.ScopedSessions)
	}
	if len(settings.ScopedGrants) == 0 {
		return nil, fmt.Errorf("scoped-sessions: %s needs scoped-grants: with no grant every scoped call is refused", settings.ScopedSessions)
	}
	grants, err := json.Marshal(settings.ScopedGrants)
	if err != nil {
		return nil, err
	}
	if _, err := sessions.ParseGrants(string(grants)); err != nil {
		return nil, err
	}
	envs := []*resources.EnvironmentVariable{
		resources.Env("SOS_SCOPED_SESSIONS", settings.ScopedSessions),
		resources.Env("SOS_SCOPED_GRANTS", string(grants)),
	}
	if settings.ScopedPresign {
		envs = append(envs, resources.Env("SOS_SCOPED_PRESIGN", "true"))
	}
	return envs, nil
}
