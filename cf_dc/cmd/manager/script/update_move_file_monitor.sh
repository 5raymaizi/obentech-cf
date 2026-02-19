#!/bin/bash

source_file="/data/vhosts/cf_dc/cf_stats_monitor_dc.20250905"
target_directories=(
    "/data/vhosts/cf_dc/monitor_dcpro1"
    "/data/vhosts/cf_dc/monitor_dcpro2"
    "/data/vhosts/cf_dc/monitor_dcpro3"
    "/data/vhosts/cf_dc/monitor_dcpro4"
    "/data/vhosts/cf_dc/monitor_dcpro5"
    "/data/vhosts/cf_dc/monitor_dcpro6"
    "/data/vhosts/cf_dc/monitor_dcpro7"
    "/data/vhosts/cf_dc/monitor_dcpro8"
    "/data/vhosts/cf_dc/monitor_dcpro9"
    "/data/vhosts/cf_dc/monitor_dcpro10"
    "/data/vhosts/cf_dc/monitor_dcpro11"
    "/data/vhosts/cf_dc/monitor_dcpro12"
    "/data/vhosts/cf_dc/monitor_dcob1"
)
symlink_name="cf_stats_monitor_dc"

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
