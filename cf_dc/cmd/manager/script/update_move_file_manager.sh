#!/bin/bash

source_file="/data/vhosts/cf_dc/manager_dc.20251018"
target_directories=(
    "/data/vhosts/cf_dc/manager_dcpro1"
    "/data/vhosts/cf_dc/manager_dcpro2"
    "/data/vhosts/cf_dc/manager_dcpro3"
    "/data/vhosts/cf_dc/manager_dcpro4"
    "/data/vhosts/cf_dc/manager_dcpro5"
    "/data/vhosts/cf_dc/manager_dcpro6"
    "/data/vhosts/cf_dc/manager_dcpro7"
    "/data/vhosts/cf_dc/manager_dcpro8"
    "/data/vhosts/cf_dc/manager_dcpro9"
    "/data/vhosts/cf_dc/manager_dcpro10"
    "/data/vhosts/cf_dc/manager_dcpro11"
    "/data/vhosts/cf_dc/manager_dcpro12"
    "/data/vhosts/cf_dc/manager_dcpro13"
    "/data/vhosts/cf_dc/manager_dcpro14"
    "/data/vhosts/cf_dc/manager_dcpro15"
    "/data/vhosts/cf_dc/manager_dcpro16"
    "/data/vhosts/cf_dc/manager_dcpro17"
    "/data/vhosts/cf_dc/manager_dcob1"
)
symlink_name="manager"

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
