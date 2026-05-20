#!/bin/bash
# 一键发布脚本（对标 miaomiaowux/scripts/release.sh）
# 流程：bump version -> 更新 README changelog -> commit -> tag -> push -> 创建 GitHub Release
# 用法：
#   bash scripts/release.sh            # patch +1（默认）
#   bash scripts/release.sh minor      # minor +1
#   bash scripts/release.sh major      # major +1
#   bash scripts/release.sh 1.2.3      # 指定版本号
#
# 说明：mmw-agent 无前端/package.json，版本号以 git tag 为准。
# 推送 tag 后由 .github/workflows/build.yml 自动编译 4 个平台二进制并上传到本 Release。

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

REPO="iluobei/mmw-agent"

# 必须在 main 分支、工作区干净
BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ "$BRANCH" != "main" ]; then
  echo "[ERROR] 当前在 ${BRANCH} 分支，请切换到 main 再发布"
  exit 1
fi
if [ -n "$(git status --porcelain)" ]; then
  echo "[ERROR] 工作区有未提交的改动，请先提交或暂存"
  exit 1
fi

# 获取上一个 tag
PREV_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "")
if [ -z "$PREV_TAG" ]; then
  echo "[ERROR] 没有找到上一个 tag，无法生成 changelog"
  exit 1
fi

# 收集自上个 tag 以来的 commit messages（排除版本号 commit 和 merge commit）
COMMITS=$(git log "${PREV_TAG}..HEAD" --pretty=format:"- %s" --no-merges | grep -v "^- v[0-9]" | sort -u || true)
if [ -z "$COMMITS" ]; then
  echo "[SKIP] 没有新的 commit，跳过发布"
  exit 0
fi

echo "=== 变更内容（自 ${PREV_TAG}）==="
echo "$COMMITS"
echo ""

# 1. 计算新版本号
echo "[1/5] 计算版本号..."
CUR=${PREV_TAG#v}
IFS='.' read -r MAJ MIN PAT <<< "$CUR"
case "${1:-patch}" in
  major) MAJ=$((MAJ + 1)); MIN=0; PAT=0 ;;
  minor) MIN=$((MIN + 1)); PAT=0 ;;
  patch) PAT=$((PAT + 1)) ;;
  *)
    # 指定版本号（允许带或不带 v 前缀）
    EXPLICIT=${1#v}
    if ! echo "$EXPLICIT" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then
      echo "[ERROR] 无法识别的版本参数: $1（应为 patch|minor|major|X.Y.Z）"
      exit 1
    fi
    IFS='.' read -r MAJ MIN PAT <<< "$EXPLICIT"
    ;;
esac
NEW_VERSION="${MAJ}.${MIN}.${PAT}"
echo "  -> 新版本: v${NEW_VERSION}"

# 2. 更新 README changelog
echo "[2/5] 更新 README changelog..."
TODAY=$(date +%Y-%m-%d)

TMPFILE=$(mktemp)
echo "### v${NEW_VERSION} (${TODAY})" > "$TMPFILE"
echo "$COMMITS" >> "$TMPFILE"
echo "" >> "$TMPFILE"

INSERT_LINE=$(grep -n '<summary>更新日志</summary>' "$PROJECT_ROOT/README.md" | head -1 | cut -d: -f1)
if [ -z "$INSERT_LINE" ]; then
  echo "[ERROR] README.md 中未找到 '<summary>更新日志</summary>' 锚点"
  rm -f "$TMPFILE"
  exit 1
fi
INSERT_LINE=$((INSERT_LINE + 1))

{
  head -n "$INSERT_LINE" "$PROJECT_ROOT/README.md"
  cat "$TMPFILE"
  tail -n +"$((INSERT_LINE + 1))" "$PROJECT_ROOT/README.md"
} > "$PROJECT_ROOT/README.md.tmp"
mv "$PROJECT_ROOT/README.md.tmp" "$PROJECT_ROOT/README.md"
rm -f "$TMPFILE"
echo "  -> README 已更新"

# 3. commit + tag
echo "[3/5] 创建 commit 和 tag..."
git add -A
git commit -m "v${NEW_VERSION}" --no-verify
git tag "v${NEW_VERSION}"
echo "  -> tag: v${NEW_VERSION}"

# 4. push
echo "[4/5] 推送到远程..."
git push origin main
git push origin "v${NEW_VERSION}"

# 5. 创建 GitHub Release（二进制由 GitHub Action 在 tag 推送后自动编译并上传）
echo "[5/5] 创建 GitHub Release..."
RELEASE_BODY="## 更新日志

### v${NEW_VERSION} (${TODAY})
${COMMITS}

## 升级方式
在主控「服务管理」中对目标服务器点击「Agent 管理 → 升级 Agent」，或重新运行安装脚本。

二进制（linux/darwin × amd64/arm64）由 GitHub Action 自动编译并上传到本 Release。"

gh release create "v${NEW_VERSION}" \
  --repo "$REPO" \
  --title "v${NEW_VERSION}" \
  --notes "$RELEASE_BODY"

echo ""
echo "=== 发布完成! v${NEW_VERSION} ==="
echo "  Release: https://github.com/${REPO}/releases/tag/v${NEW_VERSION}"
echo "  GitHub Action 将自动编译二进制并上传到该 Release"
