package etcd

import (
	"errors"
	"fmt"
	"time"
)

const (
	// DefaultDialTimeout is the default dial timeout of etcd connection.
	DefaultDialTimeout = 5 * time.Second

	// DefaultLockLeaseTime is the default lease time of lock.
	DefaultLockLeaseTime = 30 * time.Second
)

var (
	//ErrEmptyEndpoints occurs when the endpoints in the config is empty.
	ErrEmptyEndpoints = errors.New("length of etcd endpoints must be greater than 0")
)

// Config is the configuration for creating a Client instance.
type Config struct {
	Endpoints   []string `yaml:"Endpoints"`
	DialTimeout string   `yaml:"DialTimeout"`
	Username    string   `yaml:"Username"`
	Password    string   `yaml:"Password"`

	LockLeaseTime string `yaml:"LockLeaseTime"`
}

// Validate validates this config.
func (c *Config) Validate() error {
	if len(c.Endpoints) == 0 {
		return ErrEmptyEndpoints
	}

	if _, err := time.ParseDuration(c.DialTimeout); err != nil {
		return fmt.Errorf(
			`invalid argument "%s" for "--etcd-dial-timeout" flag: %w`,
			c.DialTimeout,
			err,
		)
	}

	if _, err := time.ParseDuration(c.LockLeaseTime); err != nil {
		return fmt.Errorf(
			`invalid argument "%s" for "--etcd-lock-lease-time" flag: %w`,
			c.LockLeaseTime,
			err,
		)
	}

	return nil
}

// ParseDialTimeout returns timeout for lock.
func (c *Config) ParseDialTimeout() time.Duration {
	result, err := time.ParseDuration(c.DialTimeout)
	if err != nil {
		panic(err)
	}

	return result
}

// ParseLockLeaseTime returns lease time for lock.
func (c *Config) ParseLockLeaseTime() time.Duration {
	result, err := time.ParseDuration(c.LockLeaseTime)
	if err != nil {
		panic(err)
	}

	return result
}
