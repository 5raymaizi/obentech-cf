#!/bin/bash

source_file="/data/vhosts/cf_dc/cf_arbitrage_dc.20251018"
target_directories=(
    "/data/vhosts/cf_dc/manager_dcpro1/bin"
    "/data/vhosts/cf_dc/manager_dcpro2/bin"
    "/data/vhosts/cf_dc/manager_dcpro3/bin"
    "/data/vhosts/cf_dc/manager_dcpro4/bin"
    "/data/vhosts/cf_dc/manager_dcpro5/bin"
    "/data/vhosts/cf_dc/manager_dcpro6/bin"
    "/data/vhosts/cf_dc/manager_dcpro7/bin"
    "/data/vhosts/cf_dc/manager_dcpro8/bin"
    "/data/vhosts/cf_dc/manager_dcpro9/bin"
    "/data/vhosts/cf_dc/manager_dcpro10/bin"
    "/data/vhosts/cf_dc/manager_dcpro11/bin"
    "/data/vhosts/cf_dc/manager_dcpro12/bin"
    "/data/vhosts/cf_dc/manager_dcpro13/bin"
    "/data/vhosts/cf_dc/manager_dcpro14/bin"
    "/data/vhosts/cf_dc/manager_dcpro15/bin"
    "/data/vhosts/cf_dc/manager_dcpro16/bin"
    "/data/vhosts/cf_dc/manager_dcpro17/bin"
    "/data/vhosts/cf_dc/manager_dcob1/bin"
)
symlink_name="cf_arbitrage_dc"

# 移动文件到目标目录并创建软链接
for directory in "${target_directories[@]}"; do
    # 判断目标目录是否存在
    if [ ! -d "$directory" ]; then
        echo "目标目录 $directory 不存在"
        continue
    fi

    # 获取目标文件路径
    target_file="$directory/$(basename "$source_file")"

    # 复制文件到目标目录
    cp -f "$source_file" "$target_file"

    # 创建软链接
    ln -snf "$target_file" "$directory/$symlink_name"

    echo "已将文件 $source_file 复制到目录 $directory，并在目录中创建了软链接 $symlink_name 指向 $target_file"
done
