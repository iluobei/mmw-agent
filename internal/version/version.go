// Package version 维护 mmw-agent 的版本号。
// 升级到 GitHub release 时 bump 这里 + tag GitHub release(tag_name 取 "v"+Version)。
// 主控通过 /api/child/system-info 拿到这个值与 GitHub latest tag 比对,触发升级提示。
package version

const Version = "0.2.0"
