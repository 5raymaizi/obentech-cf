import os

# 获取当前脚本所在目录，并指向其下的app文件夹
current_dir = os.path.dirname(os.path.abspath(__file__))
directory = os.path.join(current_dir, "app")
# 检查目录是否存在
if not os.path.exists(directory):
    print(f"错误：目录 '{directory}' 不存在")
else:
    # 遍历目录
    subdirectories = [d for d in os.listdir(directory) if os.path.isdir(os.path.join(directory, d))]
    # 将单引号替换为双引号
    output = str(subdirectories).replace("'", '"')
    # 输出
    print(output)
