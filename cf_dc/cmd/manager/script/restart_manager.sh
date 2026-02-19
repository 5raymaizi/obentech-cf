#!/bin/bash

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
)

# 移动文件到目标目录并创建软链接
for dir in "${target_directories[@]}"; do
  # 进入目录
  cd "$dir" || continue
  directory_name=$(basename "$dir")
  current_time=$(date +"%Y-%m-%d %H:%M:%S")

  # 检查程序是否已经在运行，匹配完整命令
  pid=$(pgrep -f "./manager $directory_name$")
  if [ -n "$pid" ]; then
    echo "[$current_time] The program './manager $directory_name' is already running with PID $pid. Skipping..."
  else
    # 执行 ./manager
    echo "[$current_time] Executing './manager $directory_name' in $dir"
    # 检查目录中是否存在 secrets.yaml 文件
    if [ -f "secrets.yaml" ]; then
      # 存在 secrets.yaml，添加环境变量
      SECRET_FILE=secrets.yaml nohup ./manager "$directory_name" >> nohup."$directory_name" 2>&1 &
    else
      nohup ./manager "$directory_name" >> nohup."$directory_name" 2>&1 &
    fi
    echo "[$current_time] Execution result: $?"
  fi

  # 回到脚本所在目录
  cd - > /dev/null
done
