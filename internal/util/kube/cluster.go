package kube

import (
	"os"
	"strings"

	"k8s.io/client-go/rest"
)

func DetectClusterName(restConfig *rest.Config) string {
	// 1. ENV
	if cluster := os.Getenv("CLUSTER_NAME"); cluster != "" {
		return cluster
	}

	// 2. RestConfig.Host
	if restConfig != nil && restConfig.Host != "" {
		// Es: https://123.45.67.89:6443 -> 123.45.67.89
		host := restConfig.Host
		host = strings.TrimPrefix(host, "https://")
		host = strings.TrimPrefix(host, "http://")
		parts := strings.Split(host, ":")
		return parts[0]
	}

	// 3. local hostname (fallback)
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown-cluster"
	}
	return hostname
}
