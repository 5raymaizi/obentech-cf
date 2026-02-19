package config

import "strings"

// GenerateUniqueName 生成等同于mmadmin环境的env名
func GenerateUniqueName(account string) string {
	return strings.Split(strings.SplitN(account, `_`, 2)[1], `(`)[0]
}
