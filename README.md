# 百川数据助手使用说明

## 项目概述

百川数据助手是一款自动化邮件发送工具，基于Python开发，支持API数据获取、Excel生成、邮件自动发送、Windows任务计划等功能。采用单文件绿色版设计，可在Windows 10/11系统上即插即用。

## 功能特性

- ✅ API数据获取与处理
- ✅ Excel文件自动生成
- ✅ 邮件自动发送（支持HTML正文、附件、抄送）
- ✅ 敏感信息加密存储
- ✅ Windows任务计划集成
- ✅ 任务锁机制防并发
- ✅ 重试机制保证可靠性
- ✅ 完整的GUI配置界面（CustomTkinter）
- ✅ 命令行接口支持Headless模式
- ✅ 完整的日志记录系统

## 文件结构

```
百川数据助手/
├─ app.py                    # 主程序文件
├─ requirements.txt          # 依赖包列表
├─ config.json              # 任务配置文件
├─ secret.key               # 加密密钥文件（隐藏属性）
├─ app.log                  # 运行日志
├─ data/                    # Excel附件输出目录
│   ├─ <任务名>_<日期>.xlsx
│   └─ <任务名>.lock        # 任务锁文件
└─ README.md                # 使用说明
```

## 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方法

### 1. GUI模式（推荐）

直接运行程序启动图形界面：

```bash
python app.py
```

GUI界面提供以下功能：

- **任务管理首页**：卡片式展示所有任务，支持新建、编辑、删除、测试运行
- **三步骤配置向导**：
  - 步骤1：API配置（URL、Headers、测试连接）
  - 步骤2：数据预览（文件名模式、数据预览）
  - 步骤3：邮箱配置（发件人、收件人、邮件内容、测试邮件）
- **定时任务管理**：一键注册/注销Windows任务计划
- **实时状态显示**：任务执行状态、定时任务状态

### 2. 命令行模式

```bash
# 查看所有任务
python app.py --list-tasks

# 测试运行指定任务
python app.py --test-task "任务名称"

# 注册定时任务
python app.py --register-task "任务名称"

# 注销定时任务
python app.py --unregister-task "任务名称"

# Headless模式运行（供任务计划调用）
python app.py --headless "任务名称"
```

### 3. 配置任务

#### GUI配置（推荐）

1. 启动程序：`python app.py`
2. 点击"新建任务"按钮
3. 按照三步骤向导配置：
   - **API配置**：填写API地址、Headers（支持加密存储）
   - **数据预览**：设置文件名模式，预览数据
   - **邮箱配置**：配置发件人、收件人、邮件内容
4. 点击"保存"完成配置

#### 手动配置

程序启动时会自动创建默认配置文件 `config.json`。您可以手动编辑该文件来配置任务：

```json
{
  "version": "1.0.0",
  "tasks": [
    {
      "name": "任务名称",
      "api_config": {
        "url": "API地址",
        "headers": {
          "appKey": "API密钥",
          "appSecret": "API密钥"
        },
        "timeout": 30,
        "verify_ssl": true
      },
      "data_config": {
        "required_fields": ["必要字段1", "必要字段2"],
        "filename_pattern": "{taskName}_{date}.xlsx"
      },
      "email_config": {
        "sender": {
          "email": "发件人邮箱",
          "password_encrypted": "加密后的密码"
        },
        "recipients": {
          "to": ["收件人1@example.com", "收件人2@example.com"],
          "cc": ["抄送人@example.com"],
          "bcc": []
        },
        "subject": "邮件主题 - {date}",
        "body": "<p>邮件正文内容</p>",
        "attachment_name": "{taskName}_{date}.xlsx"
      },
      "schedule_config": {
        "enabled": false,
        "time": "18:00",
        "frequency": "DAILY"
      },
      "status": "active"
    }
  ]
}
```

### 4. 加密配置

程序会自动生成加密密钥文件 `secret.key`，并自动加密以下字段：

- API密钥（appKey、appSecret等）
- 邮箱密码
- 其他敏感信息

### 5. 占位符替换

支持在以下字段中使用占位符：

- `{date}` - 自动替换为当前日期（格式：YYYYMMDD）
- `{taskName}` - 自动替换为任务名称

## Windows任务计划

### GUI方式（推荐）

在任务管理界面，点击任务卡片上的"注册定时"按钮即可自动创建Windows任务计划。

### 命令行方式

```bash
# 注册任务（每天18:00自动执行）
python app.py --register-task "任务名称"

# 注销任务
python app.py --unregister-task "任务名称"

# 查看所有KW_前缀的任务
schtasks /query /fo LIST | findstr KW_
```

任务计划会自动创建以下命令：

```
C:\path\to\app.exe --headless "任务名称"
```

## 日志系统

程序使用以下格式记录日志：

```
2025-10-30 18:00:01 | INFO | 开始执行任务: 任务名称
2025-10-30 18:00:02 | INFO | API数据获取成功，共 1000 行数据
2025-10-30 18:00:03 | INFO | Excel文件生成成功: 任务名称_20251030.xlsx (1.2 MB)
2025-10-30 18:00:04 | INFO | 邮件发送成功，收件人: 3人, 抄送: 1人
```

## 错误处理

- **API请求失败**：自动重试3次，间隔5秒
- **邮件发送失败**：自动重试3次，间隔5秒
- **任务并发**：通过锁文件防止同一任务同时运行
- **数据校验**：检查必要字段是否完整

## 安全特性

1. **加密存储**：敏感信息使用AES-128加密
2. **日志脱敏**：敏感字段在日志中显示为 `***`
3. **任务锁**：防止任务并发执行
4. **路径安全**：自动处理长路径问题
5. **GUI安全**：密码输入框使用掩码显示

## GUI界面说明

### 主界面

- **任务列表**：卡片式展示所有任务
- **任务信息**：显示任务名称、API域名、收件人数量
- **状态指示**：绿色=启用，红色=禁用
- **操作按钮**：编辑、测试运行、定时任务、删除

### 配置向导

- **步骤指示器**：清晰显示当前配置步骤
- **API配置**：支持多Header配置，实时测试连接
- **数据预览**：实时预览API返回的数据
- **邮箱配置**：支持HTML邮件正文，实时测试邮件

### 任务管理

- **一键操作**：注册/注销定时任务
- **批量管理**：支持多个任务的统一管理
- **状态同步**：实时同步Windows任务计划状态

## 常见问题

### Q: 程序启动时提示缺少CustomTkinter

A: 请运行 `pip install customtkinter` 安装GUI依赖

### Q: GUI界面无法显示

A: 确保安装了CustomTkinter：

```bash
pip install customtkinter
```

### Q: 如何添加新任务

A:

1. GUI方式：点击"新建任务"，按照三步骤向导配置
2. 手动方式：编辑 `config.json` 文件，在 `tasks` 数组中添加新的任务配置

### Q: 如何加密邮箱密码

A: 程序提供 `encrypt_data()` 函数，您可以使用以下代码加密：

```python
from app import encrypt_data
encrypted = encrypt_data("your_password")
```

### Q: 任务计划不执行

A: 检查以下几点：

1. 确保Python路径正确
2. 确保工作目录设置正确
3. 检查日志文件 `app.log` 中的错误信息
4. 确保GUI中已启用定时任务

### Q: API连接失败

A: 检查以下几点：

1. API地址是否正确
2. Headers配置是否正确
3. 网络连接是否正常
4. 使用GUI中的"测试API连接"功能验证配置

## 技术支持

如有问题，请检查：

1. `app.log` 日志文件
2. 确认API地址和密钥正确
3. 确认邮箱配置正确
4. 确认网络连接正常
5. 使用GUI中的测试功能验证配置

## 版本信息

- 版本：1.0.0
- 开发语言：Python 3.9+
- 测试环境：Windows 10/11
- GUI框架：CustomTkinter
