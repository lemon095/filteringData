#!/bin/bash

# 跨平台二进制打包脚本
# 支持 Linux、macOS Intel、macOS ARM (M1/M2) 等平台

set -e

# 项目名称
PROJECT_NAME="filteringData"
VERSION="1.0.0"

# 创建输出目录
BUILD_DIR="build"
mkdir -p $BUILD_DIR

echo "🚀 开始跨平台打包 $PROJECT_NAME v$VERSION..."

# 获取当前时间作为构建时间
BUILD_TIME=$(date -u '+%Y-%m-%d_%H-%M-%S_UTC')

# 通用构建参数
COMMON_FLAGS="-ldflags=-s -w -X main.BuildTime=$BUILD_TIME"

# 构建函数
build_binary() {
    local os=$1
    local arch=$2
    local suffix=$3
    local output_name="${PROJECT_NAME}${suffix}"
    
    echo "📦 构建 $os/$arch..."
    
    # 设置环境变量
    export GOOS=$os
    export GOARCH=$arch
    export CGO_ENABLED=0  # 静态链接，避免依赖问题
    
    # 执行构建
    go build -ldflags="-s -w -X main.BuildTime=$BUILD_TIME" -o "$BUILD_DIR/$output_name" .
    
    # 检查构建结果
    if [ -f "$BUILD_DIR/$output_name" ]; then
        echo "✅ $output_name 构建成功"
        # 显示文件大小
        ls -lh "$BUILD_DIR/$output_name"
    else
        echo "❌ $output_name 构建失败"
        exit 1
    fi
}

# 清理之前的构建
echo "🧹 清理之前的构建文件..."
rm -rf $BUILD_DIR
mkdir -p $BUILD_DIR

# 构建各平台版本

# Linux 平台
echo ""
echo "🐧 构建 Linux 版本..."
build_binary "linux" "amd64" ""      # Linux x86_64
build_binary "linux" "arm64" ""      # Linux ARM64
build_binary "linux" "386" ""        # Linux i386

# macOS 平台
echo ""
echo "🍎 构建 macOS 版本..."
build_binary "darwin" "amd64" ""     # macOS Intel
build_binary "darwin" "arm64" ""     # macOS ARM (M1/M2)

# Windows 平台 (可选)
echo ""
echo "🪟 构建 Windows 版本..."
build_binary "windows" "amd64" ".exe"    # Windows x86_64
build_binary "windows" "arm64" ".exe"    # Windows ARM64
build_binary "windows" "386" ".exe"      # Windows i386

# 创建发布包
echo ""
echo "📦 创建发布包..."

# 创建版本目录
RELEASE_DIR="$BUILD_DIR/release_${VERSION}_${BUILD_TIME}"
mkdir -p $RELEASE_DIR

# 复制二进制文件
cp $BUILD_DIR/${PROJECT_NAME}* $RELEASE_DIR/

# 复制配置文件
if [ -f "config.yaml" ]; then
    cp config.yaml $RELEASE_DIR/
    echo "📄 复制配置文件 config.yaml"
fi

# 复制 README 文件
if [ -f "README.md" ]; then
    cp README.md $RELEASE_DIR/
    echo "📖 复制 README.md"
fi

# 创建运行脚本
cat > "$RELEASE_DIR/run.sh" << 'EOF'
#!/bin/bash
# 自动检测平台并运行对应的二进制文件

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

# 映射架构名称
case $ARCH in
    x86_64) ARCH="amd64" ;;
    aarch64) ARCH="arm64" ;;
    i386|i686) ARCH="386" ;;
esac

# 查找对应的二进制文件
BINARY_NAME="filteringData"

# 检查文件是否存在
if [ -f "$SCRIPT_DIR/$BINARY_NAME" ]; then
    echo "🚀 运行 $BINARY_NAME..."
    chmod +x "$SCRIPT_DIR/$BINARY_NAME"
    "$SCRIPT_DIR/$BINARY_NAME"
else
    echo "❌ 未找到适合当前平台的二进制文件: $BINARY_NAME"
    echo "📋 可用的二进制文件:"
    ls -la "$SCRIPT_DIR"/filteringData*
    exit 1
fi
EOF

chmod +x "$RELEASE_DIR/run.sh"

# 创建 Windows 批处理文件
cat > "$RELEASE_DIR/run.bat" << 'EOF'
@echo off
REM 自动检测平台并运行对应的二进制文件

set SCRIPT_DIR=%~dp0
set OS=windows

REM 检测架构 (简化版本)
if "%PROCESSOR_ARCHITECTURE%"=="AMD64" (
    set ARCH=amd64
) else if "%PROCESSOR_ARCHITECTURE%"=="ARM64" (
    set ARCH=arm64
) else (
    set ARCH=386
)

set BINARY_NAME=filteringData.exe

if exist "%SCRIPT_DIR%%BINARY_NAME%" (
    echo 🚀 运行 %BINARY_NAME%...
    "%SCRIPT_DIR%%BINARY_NAME%"
) else (
    echo ❌ 未找到适合当前平台的二进制文件: %BINARY_NAME%
    echo 📋 可用的二进制文件:
    dir "%SCRIPT_DIR%"filteringData.exe
    pause
)
EOF

# 创建压缩包
echo ""
echo "🗜️  创建压缩包..."

cd $BUILD_DIR
tar -czf "${PROJECT_NAME}_${VERSION}_${BUILD_TIME}_all_platforms.tar.gz" "release_${VERSION}_${BUILD_TIME}"
zip -r "${PROJECT_NAME}_${VERSION}_${BUILD_TIME}_all_platforms.zip" "release_${VERSION}_${BUILD_TIME}"
cd ..

echo ""
echo "🎉 跨平台打包完成！"
echo ""
echo "📁 构建文件位置: $BUILD_DIR/"
echo "📦 发布包位置: $RELEASE_DIR/"
echo "🗜️  压缩包位置:"
echo "   - $BUILD_DIR/${PROJECT_NAME}_${VERSION}_${BUILD_TIME}_all_platforms.tar.gz"
echo "   - $BUILD_DIR/${PROJECT_NAME}_${VERSION}_${BUILD_TIME}_all_platforms.zip"
echo ""
echo "🚀 使用方法:"
echo "   Linux/macOS: ./run.sh"
echo "   Windows: run.bat"
echo ""
echo "📋 支持的平台:"
echo "   - Linux (x86_64, ARM64, i386)"
echo "   - macOS (Intel, M1/M2)"
echo "   - Windows (x86_64, ARM64, i386)" 