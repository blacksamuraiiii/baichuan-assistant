# 百川数据助手

百川数据助手是一款基于Python的自动化数据处理与邮件发送工具。它通过图形化界面（GUI）简化了从API获取数据、生成Excel报表到邮件发送的全过程，并集成了Windows任务计划功能以实现无人值守的自动化。

**对于非技术人员，我们提供了详细的图形化界面操作手册，请查阅 [《项目操作文档.md》](./项目操作文档.md)。**

## 技术栈

- **GUI框架**: `CustomTkinter` - 一个基于Tkinter的现代UI库。
- **数据处理**: `pandas` - 用于高效处理从API获取的数据，并生成Excel文件。
- **配置文件**: `JSON` 格式，用于存储任务配置，易于读写和扩展。
- **加密**: `cryptography` (Fernet) - 用于对API密钥、邮箱密码等敏感信息进行AES-128对称加密。
- **Windows任务计划**: 通过调用系统 `schtasks.exe` 命令实现任务的注册、查询和删除。
- **打包**: `PyInstaller` - 用于将Python程序打包成单文件可执行程序（`.exe`）。

## 核心逻辑实现

### 1. 配置管理

- 所有任务配置存储在根目录的 `config.json` 文件中。
- 程序启动时会读取该文件，并将其内容解析为任务对象。
- GUI中的任何修改都会实时更新到 `config.json`。

### 2. 敏感信息加密

- 首次运行时，程序会在根目录生成一个 `secret.key` 文件，作为AES加密的密钥。
- `config.json` 中所有标记为敏感的字段（如 `password`, `app_secret`）在保存时都会被自动加密。
- 读取配置时，程序会使用 `secret.key` 自动解密这些字段，供程序在内存中使用。

### 3. 任务执行流程

- 任务可以通过GUI的“测试运行”按钮或命令行的 `--headless` 参数触发。
- **任务锁**：执行前，会在 `locks/` 目录下创建一个 `<task_name>.lock` 文件，防止同一任务并发执行。任务结束后，锁文件被自动删除。
- **数据获取**：使用 `requests` 库调用用户配置的API，支持设置请求头和超时。
- **数据处理**：使用 `pandas` 将返回的JSON数据转换为DataFrame，并使用 `to_excel` 方法生成包含多个Sheet的Excel文件。
- **邮件发送**：使用 `smtplib` 和 `email` 模块发送邮件。支持HTML格式正文，并能将 `pandas` DataFrame渲染为HTML表格嵌入邮件中。
- **日志记录**：所有操作都会记录在 `app.log` 中，使用 `logging` 模块实现，并按天轮转。

### 4. Windows任务计划集成

- GUI通过调用 `subprocess` 模块执行 `schtasks.exe` 命令来管理定时任务。
- 创建的任务会执行 `百川数据助手.exe --headless "任务名称"` 命令，以无头模式在后台运行。

## 开发与使用

### 文件结构

```
百川数据助手/
├─ app.py                    # 主程序文件
├─ requirements.txt          # 依赖包列表
├─ config.json              # 任务配置文件
├─ secret.key               # 加密密钥文件（自动生成，隐藏）
├─ app.log                  # 运行日志
├─ locks/                   # 任务锁文件目录（自动生成）
└─ README.md                # 本说明文件
```

### 开发环境设置

1.  克隆仓库。
2.  创建并激活虚拟环境（推荐）。
3.  安装依赖：
    ```bash
    pip install -r requirements.txt
    ```
4.  运行程序：
    ```bash
    python app.py
    ```

### 命令行接口

程序提供以下命令行参数，方便调试和自动化调用：

```bash
# 以无头模式运行指定任务（主要供Windows任务计划调用）
python app.py --headless "你的任务名称"

# 立即测试运行一个任务
python app.py --test-task "你的任务名称"

# 列出所有已配置的任务
python app.py --list-tasks

# 注册一个定时任务
python app.py --register-task "你的任务名称"

# 注销一个已注册的定时任务
python app.py --unregister-task "你的任务名称"
```

### 技术支持

如遇问题，请将 `app.log` 文件发送给开发人员 **黄维申** (`huangweishen.js@chinatelecom.cn`)。
