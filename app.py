# -*- coding: utf-8 -*-
"""
@Time : 2025/10/30 14:00
@Author : black_samurai
@File : app.py
@description : 百川数据助手 - 自动化邮件发送工具
实现功能：
- API数据获取与Excel生成
- 加密存储敏感信息
- 邮件自动发送
- Windows任务计划集成
- GUI配置界面
- Headless模式运行

⚠️ 重要提示：本工具仅针对江苏电信百川平台API开发，使用前请确认是否有平台访问权限
"""

import os
import sys
import json
import time
import logging
import argparse
import subprocess
import pandas as pd
import requests
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

# GUI相关导入（可选，如果安装了CustomTkinter）
GUI_AVAILABLE = False
try:
    import customtkinter as ctk
    from customtkinter import CTk, CTkFrame, CTkButton, CTkLabel, CTkEntry, CTkTextbox, CTkComboBox, CTkCheckBox, CTkProgressBar
    from customtkinter import CTkTabview, CTkScrollableFrame, CTkToplevel, CTkRadioButton
    from CTkMessagebox import CTkMessagebox
    GUI_AVAILABLE = True
    print("GUI功能已启用")
except ImportError as e:
    print(f"警告: CustomTkinter或CTkMessagebox未安装或导入失败: {e}")
    print("GUI功能不可用，请运行: pip install customtkinter CTkMessagebox")

# 配置文件路径
# 区分内部和外部路径，以适应PyInstaller打包
if getattr(sys, 'frozen', False):
    # 打包后：内部文件在_MEIPASS临时目录，外部文件在exe同级目录
    INTERNAL_DIR = Path(sys._MEIPASS) if hasattr(sys, '_MEIPASS') else Path(sys.executable).parent
    EXTERNAL_DIR = Path(sys.executable).parent
else:
    # 直接运行时：所有文件都在脚本所在目录
    INTERNAL_DIR = Path(__file__).parent
    EXTERNAL_DIR = Path(__file__).parent

CONFIG_FILE = EXTERNAL_DIR / "config.json"
SECRET_KEY_FILE = INTERNAL_DIR / "secret.key"
LOG_FILE = EXTERNAL_DIR / "app.log"

# ==================== 内置默认配置 ====================
# 为了减少暴露的文件，将默认配置嵌入代码中
DEFAULT_CONFIG_TEMPLATE = {
    "version": "1.0.0",
    "tasks": [],
    "settings": {
        "default_smtp_server": "smtp.chinatelecom.cn",
        "default_smtp_port": 465,
        "default_timeout": 30,
        "retry_attempts": 3,
        "retry_delay": 5
    }
}

# 内置示例任务配置（可选，首次运行时提示用户配置）
EXAMPLE_TASK_CONFIG = {
    "name": "示例任务",
    "api_config": {
        "url": "https://api.example.com/data",
        "headers": {
            "Authorization": "Bearer your_token_here"
        },
        "timeout": 30,
        "verify_ssl": True,
        "proxy": None
    },
    "data_config": {
        "required_fields": [],
        "preview_rows": 10,
        "filename_pattern": "{taskName}_{date}.xlsx"
    },
    "email_config": {
        "sender": {
            "email": "your_email@company.com",
            "password": "your_password_here"
        },
        "recipients": {
            "to": ["recipient@company.com"],
            "cc": [],
            "bcc": []
        },
        "subject": "数据报表 - {date}",
        "body": "<p>您好，附件是 {taskName} 的数据报表，请查收。</p>",
        "attachment_name": "{taskName}_{date}.xlsx"
    },
    "schedule_config": {
        "enabled": False,
        "time": "18:00",
        "frequency": "DAILY"
    },
    "status": "active"
}

# 任务配置模板 - 支持多API配置
TASK_TEMPLATE = {
    "name": "",
    "api_configs": [
        {
            "name": "API1",
            "url": "",
            "headers": {
                "appKey": "",
                "appSecret": ""
            },
            "timeout": 30,
            "verify_ssl": True,
            "proxy": None
        }
    ],
    "data_config": {
        "required_fields": [],
        "preview_rows": 10,
        "filename_pattern": "{taskName}_{date}.xlsx",
        "sheet_names": ["Sheet1"]  # 默认sheet名称
    },
    "email_config": {
        "sender": {
            "email": "",
            "password": ""  # 明文存储
        },
        "recipients": {
            "to": [],  # 收件人列表
            "cc": [],  # 抄送列表
            "bcc": []  # 密送列表
        },
        "subject": "数据报表 - {date}",
        "body": "<p>您好，附件是 {taskName} 的数据报表，请查收。</p>",
        "attachment_name": "{taskName}_{date}.xlsx"
    },
    "schedule_config": {
        "enabled": False,
        "time": "18:00",
        "frequency": "DAILY"
    },
    "status": "active"
}

# ==================== 日志配置 ====================
def setup_logging():
    """配置日志系统"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# ==================== 全局缓存 ====================
# 用于缓存多个任务的多个DataFrame，避免重复API请求
# 缓存结构: {task_name: {api_name: df, ...}, ...}
TASK_DATA_CACHE = {}

def get_cached_dataframe(task_name: str, api_name: str = "API1") -> Optional[pd.DataFrame]:
    """获取缓存的DataFrame"""
    task_cache = TASK_DATA_CACHE.get(task_name, {})
    return task_cache.get(api_name)

def cache_dataframe(task_name: str, api_name: str, df: pd.DataFrame):
    """缓存DataFrame"""
    if task_name not in TASK_DATA_CACHE:
        TASK_DATA_CACHE[task_name] = {}
    TASK_DATA_CACHE[task_name][api_name] = df

def clear_task_cache(task_name: str):
    """清除指定任务的缓存"""
    if task_name in TASK_DATA_CACHE:
        del TASK_DATA_CACHE[task_name]

def clear_api_cache(task_name: str, api_name: str):
    """清除指定任务的指定API缓存"""
    if task_name in TASK_DATA_CACHE:
        if api_name in TASK_DATA_CACHE[task_name]:
            del TASK_DATA_CACHE[task_name][api_name]
        if not TASK_DATA_CACHE[task_name]:  # 如果该任务没有其他API缓存，删除整个任务缓存
            del TASK_DATA_CACHE[task_name]

def clear_all_cache():
    """清除所有缓存"""
    TASK_DATA_CACHE.clear()

# ==================== 加密模块 ====================
def ensure_secret_key():
    """确保加密密钥存在，不存在则生成"""
    if not SECRET_KEY_FILE.exists():
        # 如果是打包环境，密钥应该在exe中，这里生成一个临时的
        if getattr(sys, 'frozen', False):
            from cryptography.fernet import Fernet
            key = Fernet.generate_key()
            # 在打包环境中，密钥文件在临时目录，不需要设置隐藏属性
            SECRET_KEY_FILE.write_bytes(key)
            logger.info("在打包环境中生成临时加密密钥")
            return key
        else:
            # 非打包环境，按原逻辑处理
            from cryptography.fernet import Fernet
            key = Fernet.generate_key()
            SECRET_KEY_FILE.write_bytes(key)
            # 设置隐藏属性
            subprocess.run(['attrib', '+H', str(SECRET_KEY_FILE)], shell=True)
            logger.info("生成新的加密密钥")
            return key
    return SECRET_KEY_FILE.read_bytes()

def encrypt_data(data: str) -> str:
    """加密数据"""
    from cryptography.fernet import Fernet
    key = ensure_secret_key()
    fernet = Fernet(key)
    encrypted = fernet.encrypt(data.encode())
    return encrypted.decode()

def decrypt_data(encrypted_data: str) -> str:
    """解密数据"""
    from cryptography.fernet import Fernet
    key = ensure_secret_key()
    fernet = Fernet(key)
    try:
        decrypted = fernet.decrypt(encrypted_data.encode())
        return decrypted.decode()
    except Exception as e:
        logger.error(f"解密失败: {e}")
        raise ValueError("解密失败，请检查密钥")

# ==================== 配置管理 ====================
def load_config() -> Dict:
    """加载配置文件"""
    if not CONFIG_FILE.exists():
        return DEFAULT_CONFIG_TEMPLATE.copy()
    
    try:
        config_data = json.loads(CONFIG_FILE.read_text(encoding='utf-8'))
        # 确保配置结构完整
        for key, value in DEFAULT_CONFIG_TEMPLATE.items():
            if key not in config_data:
                config_data[key] = value
        return config_data
    except Exception as e:
        logger.error(f"加载配置失败: {e}")
        return DEFAULT_CONFIG_TEMPLATE.copy()

def save_config(config: Dict):
    """保存配置文件"""
    try:
        CONFIG_FILE.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding='utf-8')
        logger.info("配置保存成功")
    except Exception as e:
        logger.error(f"保存配置失败: {e}")
        raise

def get_task_config(task_name: str) -> Optional[Dict]:
    """获取指定任务配置"""
    config = load_config()
    for task in config.get("tasks", []):
        if task.get("name") == task_name:
            return task
    return None

def add_task_config(task_config: Dict):
    """添加新任务配置"""
    config = load_config()
    
    # 向后兼容性：将旧版本的api_config转换为新的api_configs格式
    if "api_config" in task_config and "api_configs" not in task_config:
        # 将旧的api_config转换为新的api_configs数组格式
        old_api_config = task_config.pop("api_config")
        task_config["api_configs"] = [old_api_config]
        
        # 为API配置添加name字段（如果不存在）
        if "name" not in old_api_config:
            old_api_config["name"] = "API1"
    
    # 向后兼容性：确保sheet_names存在
    if "data_config" in task_config and "sheet_names" not in task_config["data_config"]:
        task_config["data_config"]["sheet_names"] = ["Sheet1"]
    
    # 检查任务名是否已存在
    for i, task in enumerate(config["tasks"]):
        if task["name"] == task_config["name"]:
            config["tasks"][i] = task_config
            break
    else:
        config["tasks"].append(task_config)
    
    save_config(config)

# ==================== 任务锁机制 ====================
def get_lock_file_path(task_name: str) -> Path:
    """获取任务锁文件路径"""
    # 在打包环境中，锁文件应该在exe所在目录
    return EXTERNAL_DIR / "locks" / f"{task_name}.lock"

def acquire_lock(task_name: str) -> bool:
    """获取任务锁"""
    lock_file = get_lock_file_path(task_name)
    if lock_file.exists():
        # 检查锁文件是否过期（超过1小时）
        try:
            lock_time = datetime.fromtimestamp(lock_file.stat().st_mtime)
            if datetime.now() - lock_time > timedelta(hours=1):
                lock_file.unlink()  # 删除过期锁
            else:
                logger.info(f"任务 {task_name} 已被锁定，跳过执行")
                return False
        except:
            pass
    
    try:
        # 确保锁目录存在
        locks_dir = lock_file.parent
        if not locks_dir.exists():
            locks_dir.mkdir(parents=True, exist_ok=True)
            # 在Windows上设置为隐藏目录
            if os.name == 'nt':
                try:
                    subprocess.run(['attrib', '+H', str(locks_dir)], check=True, shell=True, capture_output=True)
                except Exception as e:
                    logger.warning(f"设置locks目录隐藏属性失败: {e}")

        lock_file.write_text(f"{os.getpid()}|{datetime.now()}", encoding='utf-8')
        logger.info(f"任务 {task_name} 锁定成功")
        return True
    except Exception as e:
        logger.error(f"锁定任务失败: {e}")
        return False

def release_lock(task_name: str):
    """释放任务锁"""
    lock_file = get_lock_file_path(task_name)
    if lock_file.exists():
        try:
            lock_file.unlink()
            logger.info(f"任务 {task_name} 锁释放")
        except Exception as e:
            logger.error(f"释放锁失败: {e}")
    
    # 清理空的locks目录
    locks_dir = lock_file.parent
    if locks_dir.exists() and not any(locks_dir.iterdir()):
        try:
            locks_dir.rmdir()
        except:
            pass  # 目录不为空或有其他问题，忽略

# ==================== 占位符替换 ====================
def replace_placeholders(text: str, task_name: str) -> str:
    """替换文本中的占位符"""
    today = date.today().strftime("%Y%m%d")
    replacements = {
        "{date}": today,
        "{taskName}": task_name
    }
    
    result = text
    for placeholder, value in replacements.items():
        result = result.replace(placeholder, value)
    
    return result

# ==================== API数据获取 ====================
def fetch_api_data(task_config: Dict, api_name: str = "API1", use_cache: bool = True) -> Optional[pd.DataFrame]:
    """从指定API获取数据"""
    task_name = task_config["name"]
    
    # 检查缓存
    if use_cache:
        cached_df = get_cached_dataframe(task_name, api_name)
        if cached_df is not None:
            logger.info(f"使用缓存的DataFrame: {task_name} - {api_name}")
            return cached_df
    
    # 获取指定API配置
    api_configs = task_config.get("api_configs", [])
    api_config = None
    for config in api_configs:
        if config.get("name") == api_name:
            api_config = config
            break
    
    if not api_config:
        logger.error(f"未找到API配置: {api_name}")
        return None
    
    url = api_config["url"]
    headers = api_config.get("headers", {})
    timeout = api_config.get("timeout", 30)
    verify_ssl = api_config.get("verify_ssl", True)
    proxy = api_config.get("proxy")
    
    # 准备代理配置
    proxies = None
    if proxy:
        proxies = {"http": proxy, "https": proxy}
    
    # 直接使用headers，不再解密
    decrypted_headers = headers
    
    logger.info(f"正在从API获取数据: {url} ({api_name})")
    
    try:
        response = requests.post(
            url,
            headers=decrypted_headers,
            timeout=timeout,
            verify=verify_ssl,
            proxies=proxies
        )
        response.raise_for_status()
        
        response_data = response.json()
        if response_data.get('success') and 'value' in response_data:
            df = pd.DataFrame(response_data['value'])
            logger.info(f"API数据获取成功: {task_name} - {api_name}, 共 {len(df)} 行数据")
            
            # 数据校验
            required_fields = task_config["data_config"].get("required_fields", [])
            if required_fields:
                missing_fields = [field for field in required_fields if field not in df.columns]
                if missing_fields:
                    logger.error(f"数据缺少必要字段: {missing_fields}")
                    return None
            
            # 缓存DataFrame
            cache_dataframe(task_name, api_name, df)
            return df
        else:
            logger.error(f"API返回数据格式不正确: {api_name}")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"API请求失败: {api_name} - {e}")
        return None
    except Exception as e:
        logger.error(f"数据处理失败: {api_name} - {e}")
        return None

def fetch_all_api_data(task_config: Dict, use_cache: bool = True) -> Dict[str, Optional[pd.DataFrame]]:
    """获取任务所有API的数据"""
    task_name = task_config["name"]
    api_configs = task_config.get("api_configs", [])
    results = {}
    
    for api_config in api_configs:
        api_name = api_config.get("name", "API1")
        df = fetch_api_data(task_config, api_name, use_cache)
        results[api_name] = df
    
    return results

# ==================== Excel文件生成 ====================
def generate_excel_file_with_sheets(task_config: Dict, data_frames: Dict[str, pd.DataFrame]) -> Optional[str]:
    """生成包含多个Sheet的Excel文件"""
    task_name = task_config["name"]
    filename_pattern = task_config["data_config"]["filename_pattern"]
    
    # 替换占位符
    filename = replace_placeholders(filename_pattern, task_name)
    
    # 使用临时目录而不是data文件夹
    import tempfile
    temp_dir = Path(tempfile.gettempdir()) / "baichuan_data_helper"
    temp_dir.mkdir(exist_ok=True)
    
    file_path = temp_dir / filename
    
    # 处理长路径
    if len(str(file_path)) > 260:
        file_path = Path(f"\\\\?\\{file_path}")
    
    try:
        # 使用ExcelWriter生成多Sheet Excel
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            sheet_names = task_config["data_config"].get("sheet_names", [])
            
            for i, (api_name, df) in enumerate(data_frames.items()):
                if df is not None and not df.empty:
                    # 获取对应的sheet名称，如果没有则使用默认名称
                    sheet_name = sheet_names[i] if i < len(sheet_names) else f"Sheet{i+1}"
                    # 确保sheet名称不超过31个字符且不包含非法字符
                    sheet_name = sheet_name[:31].replace('/', '-').replace('\\', '-').replace('?', '').replace('*', '-').replace('[', '(').replace(']', ')')
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    logger.info(f"Sheet '{sheet_name}' 写入成功: {len(df)} 行数据")
                else:
                    logger.warning(f"跳过空的DataFrame: {api_name}")
        
        file_size = file_path.stat().st_size / 1024  # KB
        logger.info(f"Excel文件生成成功: {filename} ({file_size:.1f} KB)，包含 {len(data_frames)} 个Sheet")
        return str(file_path)
    except Exception as e:
        logger.error(f"Excel文件生成失败: {e}")
        return None

def generate_excel_file(df: pd.DataFrame, task_config: Dict) -> Optional[str]:
    """生成Excel文件（向后兼容，单Sheet）"""
    # 调用新的多Sheet函数，但只传递一个DataFrame
    task_name = task_config["name"]
    sheet_names = task_config["data_config"].get("sheet_names", ["Sheet1"])
    data_frames = {"API1": df}
    
    # 临时修改配置，确保向后兼容
    original_sheet_names = task_config["data_config"].get("sheet_names", ["Sheet1"])
    task_config["data_config"]["sheet_names"] = sheet_names[:1]  # 只取第一个sheet名称
    
    try:
        result = generate_excel_file_with_sheets(task_config, data_frames)
        return result
    finally:
        # 恢复原始配置
        task_config["data_config"]["sheet_names"] = original_sheet_names

# ==================== 邮件发送 ====================
def send_email_with_attachment(task_config: Dict, attachment_path: str = None, attachment_data: bytes = None, body: str = None) -> bool:
    """发送带附件的邮件（支持文件路径或内存数据）"""
    email_config = task_config["email_config"]
    sender_config = email_config["sender"]
    recipients = email_config["recipients"]
    
    # 获取发件人密码（现在是明文）
    sender_password = sender_config.get("password", "")
    if not sender_password:
        logger.error("发件人密码为空")
        return False
    
    # 准备收件人列表
    to_list = recipients.get("to", [])
    cc_list = recipients.get("cc", [])
    bcc_list = recipients.get("bcc", [])
    
    if not to_list:
        logger.error("收件人列表为空")
        return False
    
    # 替换占位符
    subject = replace_placeholders(email_config["subject"], task_config["name"])
    
    # 使用传入的body或默认的body
    if body is None:
        body = replace_placeholders(email_config["body"], task_config["name"])
    else:
        body = replace_placeholders(body, task_config["name"])
    
    attachment_name = replace_placeholders(
        email_config["attachment_name"],
        task_config["name"]
    )
    
    # 配置SMTP
    smtp_server = task_config.get("smtp_server", "smtp.chinatelecom.cn")
    smtp_port = task_config.get("smtp_port", 465)
    
    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        from email.mime.application import MIMEApplication
        from io import BytesIO
        
        # 创建邮件
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = sender_config["email"]
        msg['To'] = ','.join(to_list)
        if cc_list:
            msg['Cc'] = ','.join(cc_list)
        
        # 添加正文
        body_mime = MIMEText(body, 'html', 'utf-8')
        msg.attach(body_mime)
        
        # 添加附件（支持内存数据或文件路径）
        if attachment_data:
            # 使用内存数据
            attachment = MIMEApplication(attachment_data, _subtype='xlsx')
            attachment.add_header('Content-Disposition', 'attachment', filename=attachment_name)
            msg.attach(attachment)
            logger.info("使用内存数据作为附件")
        elif attachment_path and Path(attachment_path).exists():
            # 使用文件路径
            with open(attachment_path, 'rb') as f:
                attachment = MIMEApplication(f.read(), _subtype='xlsx')
                attachment.add_header('Content-Disposition', 'attachment', filename=attachment_name)
                msg.attach(attachment)
            logger.info(f"使用文件作为附件: {attachment_path}")
        
        # 发送邮件
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as smtp:
            smtp.login(sender_config["email"], sender_password)
            all_recipients = to_list + cc_list + bcc_list
            smtp.sendmail(sender_config["email"], all_recipients, msg.as_string())
        
        logger.info(f"邮件发送成功，收件人: {len(to_list)}人, 抄送: {len(cc_list)}人")
        return True
        
    except Exception as e:
        logger.error(f"邮件发送失败: {e}")
        return False

def replace_sheet_variables(task_config: Dict, data_frames: Dict[str, pd.DataFrame]) -> str:
    """替换邮件正文中的Sheet变量为HTML表格"""
    body = task_config["email_config"]["body"]
    sheet_names = task_config["data_config"].get("sheet_names", [])
    
    # 为每个Sheet变量生成HTML表格
    for i, (api_name, df) in enumerate(data_frames.items()):
        if df is not None and not df.empty:
            # 获取对应的sheet名称
            sheet_name = sheet_names[i] if i < len(sheet_names) else f"Sheet{i+1}"
            
            # 生成HTML表格（取前10行）
            html_table = df.head(10).to_html(index=False, escape=True)
            
            # 替换变量
            variable_name = f"{{{sheet_name}}}"
            body = body.replace(variable_name, html_table)
    
    return body

def send_email_with_dataframes(task_config: Dict, data_frames: Dict[str, pd.DataFrame]) -> bool:
    """直接使用多个DataFrame发送邮件，无需临时文件"""
    try:
        # 替换邮件正文中的Sheet变量
        body = replace_sheet_variables(task_config, data_frames)
        
        # 将多个DataFrame转换为Excel格式的内存数据
        from io import BytesIO
        buffer = BytesIO()
        
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            sheet_names = task_config["data_config"].get("sheet_names", [])
            
            for i, (api_name, df) in enumerate(data_frames.items()):
                if df is not None and not df.empty:
                    # 获取对应的sheet名称，如果没有则使用默认名称
                    sheet_name = sheet_names[i] if i < len(sheet_names) else f"Sheet{i+1}"
                    # 确保sheet名称不超过31个字符且不包含非法字符
                    sheet_name = sheet_name[:31].replace('/', '-').replace('\\', '-').replace('?', '').replace('*', '-').replace('[', '(').replace(']', ')')
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    logger.info(f"Sheet '{sheet_name}' 写入内存: {len(df)} 行数据")
                else:
                    logger.warning(f"跳过空的DataFrame: {api_name}")
        
        attachment_data = buffer.getvalue()
        logger.info(f"多个DataFrame转换为Excel内存数据，大小: {len(attachment_data)} bytes，包含 {len(data_frames)} 个Sheet")
        
        # 发送邮件（使用替换后的正文）
        return send_email_with_attachment(task_config, attachment_data=attachment_data, body=body)
        
    except Exception as e:
        logger.error(f"DataFrame转Excel失败: {e}")
        return False

def send_email_with_dataframe(task_config: Dict, df: pd.DataFrame) -> bool:
    """直接使用DataFrame发送邮件，无需临时文件（向后兼容）"""
    # 调用新的多DataFrame函数
    data_frames = {"API1": df}
    return send_email_with_dataframes(task_config, data_frames)

# ==================== 重试机制 ====================
def with_retry(func, max_attempts: int = 3, delay: int = 5, *args, **kwargs) -> Any:
    """带重试机制的函数执行"""
    for attempt in range(max_attempts):
        try:
            result = func(*args, **kwargs)
            # For DataFrames, we need to check emptiness explicitly
            if isinstance(result, pd.DataFrame):
                # An empty dataframe is a valid result from the API, so we return it.
                return result
            # For other types, check truthiness
            elif bool(result):
                return result
        except Exception as e:
            logger.warning(f"第 {attempt + 1} 次尝试失败: {e}")
        
        if attempt < max_attempts - 1:
            logger.info(f"等待 {delay} 秒后重试...")
            time.sleep(delay)
    
    logger.error(f"所有 {max_attempts} 次尝试均失败")
    return None

# ==================== 核心执行流程 ====================
def execute_task(task_name: str) -> bool:
    """执行单个任务的完整流程"""
    logger.info(f"开始执行任务: {task_name}")
    
    # 获取任务配置
    task_config = get_task_config(task_name)
    if not task_config:
        logger.error(f"任务配置不存在: {task_name}")
        return False
    
    # 检查任务状态
    if task_config.get("status") != "active":
        logger.info(f"任务 {task_name} 已禁用")
        return False
    
    # 获取锁
    if not acquire_lock(task_name):
        return False
    
    try:
        # 1. 获取所有API数据（使用缓存）
        data_frames = with_retry(fetch_all_api_data, task_config=task_config, use_cache=True)
        if not data_frames or any(df is None for df in data_frames.values()):
            logger.error(f"任务 {task_name} 数据获取失败或数据为空")
            return False
        
        # 2. 发送邮件（直接使用DataFrame，无需临时文件）
        email_success = with_retry(send_email_with_dataframes,
                                task_config=task_config,
                                data_frames=data_frames)
        
        if email_success:
            logger.info(f"任务 {task_name} 执行成功")
            return True
        else:
            logger.error(f"任务 {task_name} 邮件发送失败")
            return False
            
    except Exception as e:
        logger.error(f"任务 {task_name} 执行过程中发生异常: {e}")
        return False
    finally:
        # 释放锁并清除缓存
        release_lock(task_name)
        clear_task_cache(task_name)

# ==================== Windows任务计划集成 ====================
def register_scheduled_task_advanced(task_name: str, frequency: str = "DAILY", time_str: str = "18:00", day_of_week: str = None) -> bool:
    """注册Windows定时任务（增强版，支持不同频率）"""
    try:
        # 构建任务计划命令
        if getattr(sys, 'frozen', False):
            exe_path = str(Path(sys.executable).resolve())
        else:
            exe_path = str(Path(__file__).resolve())
        
        task_command = f'"{exe_path}" --headless "{task_name}"'
        task_name_escaped = f"KW_{task_name.replace(' ', '_')}"
        
        # 根据频率构建参数
        if frequency == "WEEKLY":
            if not day_of_week:
                logger.error("注册每周任务时必须提供星期几")
                return False
            schedule_params = ['/SC', 'WEEKLY', '/D', day_of_week]
        else:  # DAILY
            schedule_params = ['/SC', 'DAILY']
        
        # 创建任务
        create_cmd = [
            'schtasks', '/Create', '/TN', task_name_escaped,
            '/TR', task_command,
            '/ST', time_str,
            '/F'
        ] + schedule_params
        
        logger.info(f"执行命令: {' '.join(create_cmd)}")
        
        result = subprocess.run(create_cmd, capture_output=True, shell=True)
        
        try:
            stderr_text = result.stderr.decode('utf-8', errors='ignore') if isinstance(result.stderr, bytes) else str(result.stderr or '')
        except:
            stderr_text = str(result.stderr or '')
            
        if result.returncode == 0:
            logger.info(f"定时任务注册成功: {task_name} ({frequency} {time_str} on {day_of_week})")
            return True
        else:
            logger.error(f"定时任务注册失败: {stderr_text}")
            if GUI_AVAILABLE:
                CTkMessagebox(title="定时任务注册失败", message=f"错误信息:\n{stderr_text}", icon="cancel")
            return False
            
    except Exception as e:
        logger.error(f"注册定时任务时出错: {e}")
        return False

def register_scheduled_task(task_name: str) -> bool:
    """注册Windows定时任务（兼容旧版本）"""
    # 从任务配置中获取默认设置
    task_config = get_task_config(task_name)
    if task_config and "schedule_config" in task_config:
        schedule_config = task_config["schedule_config"]
        frequency = schedule_config.get("frequency", "DAILY")
        time_str = schedule_config.get("time", "18:00")
        return register_scheduled_task_advanced(task_name, frequency, time_str)
    else:
        # 使用默认设置
        return register_scheduled_task_advanced(task_name, "DAILY", "18:00")

def get_task_status(task_name: str) -> str:
    """获取任务在Windows任务计划程序中的状态"""
    try:
        task_name_escaped = f"KW_{task_name.replace(' ', '_')}"
        
        # 查询任务状态
        result = subprocess.run(['schtasks', '/query', '/tn', task_name_escaped, '/fo', 'LIST'],
                              capture_output=True, shell=True)
        
        if result.returncode == 0:
            try:
                stdout_text = result.stdout.decode('utf-8', errors='ignore') if isinstance(result.stdout, bytes) else str(result.stdout or '')
            except:
                stdout_text = str(result.stdout or '')
            
            # 检查任务状态
            if '准备就绪' in stdout_text:
                return 'ready'
            elif '已禁用' in stdout_text:
                return 'disabled'
            elif '正在运行' in stdout_text:
                return 'running'
            else:
                return 'unknown'
        else:
            # 任务不存在
            return 'not_found'
            
    except Exception as e:
        logger.error(f"获取任务状态时出错: {e}")
        return 'error'

def create_scheduled_task(task_name: str) -> bool:
    """在Windows任务计划程序中创建新任务"""
    return register_scheduled_task_advanced(task_name)

def enable_scheduled_task(task_name: str) -> bool:
    """启用已禁用的Windows定时任务"""
    try:
        task_name_escaped = f"KW_{task_name.replace(' ', '_')}"
        
        # 检查任务是否存在
        status = get_task_status(task_name)
        if status == 'not_found':
            logger.warning(f"任务不存在，无法启用: {task_name}")
            return False
        
        # 启用任务
        enable_cmd = ['schtasks', '/change', '/tn', task_name_escaped, '/enable']
        result = subprocess.run(enable_cmd, capture_output=True, shell=True)
        
        try:
            stderr_text = result.stderr.decode('utf-8', errors='ignore') if isinstance(result.stderr, bytes) else str(result.stderr or '')
        except:
            stderr_text = str(result.stderr or '')
        
        if result.returncode == 0:
            logger.info(f"定时任务启用成功: {task_name}")
            return True
        else:
            logger.error(f"定时任务启用失败: {task_name} - {stderr_text}")
            return False
            
    except Exception as e:
        logger.error(f"启用定时任务时出错: {e}")
        return False

def disable_scheduled_task(task_name: str) -> bool:
    """禁用Windows定时任务"""
    try:
        task_name_escaped = f"KW_{task_name.replace(' ', '_')}"
        
        # 禁用任务
        disable_cmd = ['schtasks', '/change', '/tn', task_name_escaped, '/disable']
        result = subprocess.run(disable_cmd, capture_output=True, shell=True)
        
        try:
            stderr_text = result.stderr.decode('utf-8', errors='ignore') if isinstance(result.stderr, bytes) else str(result.stderr or '')
        except:
            stderr_text = str(result.stderr or '')
        
        if result.returncode == 0:
            logger.info(f"定时任务禁用成功: {task_name}")
            return True
        else:
            logger.error(f"定时任务禁用失败: {task_name} - {stderr_text}")
            return False
            
    except Exception as e:
        logger.error(f"禁用定时任务时出错: {e}")
        return False

def delete_scheduled_task(task_name: str) -> bool:
    """从Windows任务计划程序中删除任务"""
    try:
        task_name_escaped = f"KW_{task_name.replace(' ', '_')}"
        
        # 直接删除任务，使用 /F 强制删除
        delete_cmd = ['schtasks', '/delete', '/tn', task_name_escaped, '/f']
        logger.info(f"执行命令: {' '.join(delete_cmd)}")
        
        result = subprocess.run(delete_cmd, capture_output=True, shell=True)
        
        try:
            stderr_text = result.stderr.decode('utf-8', errors='ignore') if isinstance(result.stderr, bytes) else str(result.stderr or '')
        except:
            stderr_text = str(result.stderr or '')
            
        # 如果返回码为0，说明成功。如果返回码不为0，但错误信息包含“找不到”，也视为成功（任务本就不存在）
        if result.returncode == 0:
            logger.info(f"定时任务删除成功: {task_name_escaped}")
            return True
        elif "找不到" in stderr_text or "not found" in stderr_text.lower():
            logger.warning(f"尝试删除但未找到任务 (视为成功): {task_name_escaped}")
            return True
        else:
            logger.error(f"定时任务删除失败: {task_name_escaped} - {stderr_text}")
            return False
            
    except Exception as e:
        logger.error(f"删除定时任务时出错: {e}")
        return False

def unregister_scheduled_task(task_name: str) -> bool:
    """注销Windows定时任务（兼容旧版本调用）"""
    # 根据任务状态决定操作类型
    status = get_task_status(task_name)
    
    if status == 'not_found':
        # 任务不存在，直接更新配置
        config = load_config()
        for task_config in config.get("tasks", []):
            if task_config["name"] == task_name:
                task_config["schedule_config"]["enabled"] = False
                break
        save_config(config)
        return True
    elif status == 'disabled':
        # 任务已禁用，直接删除
        return delete_scheduled_task(task_name)
    else:
        # 任务已启用，先禁用再删除
        if disable_scheduled_task(task_name):
            return delete_scheduled_task(task_name)
        return False

def get_scheduled_tasks() -> List[str]:
    """获取所有KW_前缀的定时任务"""
    try:
        result = subprocess.run(
            ['schtasks', '/query', '/fo', 'LIST'],
            capture_output=True, shell=True
        )
        
        if result.returncode == 0:
            tasks = []
            # 安全地处理输出，避免编码问题
            try:
                stdout_text = result.stdout.decode('utf-8', errors='ignore') if isinstance(result.stdout, bytes) else str(result.stdout or '')
            except:
                stdout_text = str(result.stdout or '')
                
            for line in stdout_text.split('\n'):
                if 'KW_' in line:
                    # 提取任务名
                    if 'TaskName:' in line:
                        try:
                            task_name = line.split('TaskName:')[1].strip()
                            if task_name.startswith('KW_'):
                                # 去掉KW_前缀和可能的_Wxxx后缀
                                base_name = task_name[3:]  # 去掉KW_
                                if '_W' in base_name:
                                    base_name = base_name.split('_W')[0]  # 去掉星期后缀
                                if base_name not in tasks:
                                    tasks.append(base_name)
                        except:
                            continue
            return tasks
        else:
            try:
                stderr_text = result.stderr.decode('utf-8', errors='ignore') if isinstance(result.stderr, bytes) else str(result.stderr or '')
            except:
                stderr_text = str(result.stderr or '')
            logger.error(f"获取定时任务列表失败: {stderr_text}")
            return []
            
    except Exception as e:
        logger.error(f"获取定时任务时出错: {e}")
        return []

# ==================== 核心执行流程 ====================
def execute_task(task_name: str) -> bool:
    """执行单个任务的完整流程"""
    logger.info(f"开始执行任务: {task_name}")
    
    # 获取任务配置
    task_config = get_task_config(task_name)
    if not task_config:
        logger.error(f"任务配置不存在: {task_name}")
        return False
    
    # 检查任务状态
    if task_config.get("status") != "active":
        logger.info(f"任务 {task_name} 已禁用")
        return False
    
    # 获取锁
    if not acquire_lock(task_name):
        return False
    
    try:
        # 1. 获取API数据（使用缓存）
        df = with_retry(fetch_api_data, task_config=task_config)
        if df is None:
            logger.error(f"任务 {task_name} 数据获取失败或数据为空")
            return False
        
        # 2. 发送邮件（直接使用DataFrame，无需临时文件）
        email_success = with_retry(send_email_with_dataframe,
                                task_config=task_config,
                                df=df)
        
        if email_success:
            logger.info(f"任务 {task_name} 执行成功")
            return True
        else:
            logger.error(f"任务 {task_name} 邮件发送失败")
            return False
            
    except Exception as e:
        logger.error(f"任务 {task_name} 执行过程中发生异常: {e}")
        return False
    finally:
        # 释放锁并清除缓存
        release_lock(task_name)
        clear_task_cache(task_name)

# ==================== Headless模式 ====================
def run_headless(task_name: str):
    """Headless模式运行"""
    logger.info(f"Headless模式启动，执行任务: {task_name}")
    
    success = execute_task(task_name)
    if success:
        logger.info(f"Headless任务 {task_name} 完成")
        return 0
    else:
        logger.error(f"Headless任务 {task_name} 失败")
        return 1

# ==================== GUI界面模块 ====================
if GUI_AVAILABLE:
    class TaskConfigWizard(ctk.CTkToplevel):
        """任务配置向导窗口"""
        def __init__(self, parent, task_config=None):
            super().__init__(parent)
            self.parent = parent
            self.task_config = task_config or TASK_TEMPLATE.copy()
            self.preview_df = None # 用于存储预览数据
            self.title("任务配置向导" if not task_config else "编辑任务")
            self.geometry("800x600")
            self.resizable(True, True)
            
            # 步骤控制
            self.current_step = 0
            self.steps = ["API配置", "数据预览", "邮箱配置"]
            
            self.setup_ui()
            self.show_step(self.current_step)
            self.load_current_step()  # 加载现有配置
        
        def setup_ui(self):
            """设置向导界面"""
            # 顶部步骤指示器
            self.step_frame = CTkFrame(self)
            self.step_frame.pack(fill="x", padx=20, pady=10)
            
            self.step_labels = []
            for i, step_name in enumerate(self.steps):
                label = CTkButton(
                    self.step_frame,
                    text=f"{i+1}. {step_name}",
                    font=("微软雅黑", 12, "bold"),
                    fg_color="transparent",
                    hover_color="lightgray",
                    text_color="black",
                    command=lambda step=i: self.go_to_step(step)
                )
                label.grid(row=0, column=i, padx=20, sticky="w")
                self.step_labels.append(label)
            
            # 内容区域
            self.content_frame = CTkFrame(self)
            self.content_frame.pack(fill="both", expand=True, padx=20, pady=10)
            
            # 底部按钮
            self.button_frame = CTkFrame(self)
            self.button_frame.pack(fill="x", padx=20, pady=10)

            self.prev_btn = CTkButton(self.button_frame, text="上一步", command=self.prev_step, state="disabled")
            self.prev_btn.pack(side="left", padx=5)

            self.next_btn = CTkButton(self.button_frame, text="下一步", command=self.next_step)
            self.next_btn.pack(side="right", padx=5)

            self.save_btn = CTkButton(self.button_frame, text="保存", command=self.save_task, fg_color="green")

            # API配置专用按钮（移到底部）
            self.api_buttons_frame = CTkFrame(self.button_frame)
            self.api_buttons_frame.pack(side="left", padx=5)

            self.add_api_btn = CTkButton(self.api_buttons_frame, text="添加API", command=self.add_api_config)
            self.add_api_btn.pack(side="left", padx=2)

            self.delete_api_btn = CTkButton(self.api_buttons_frame, text="删除API", command=self.delete_current_api, fg_color="red")
            self.delete_api_btn.pack(side="left", padx=2)

            self.test_api_btn = CTkButton(self.api_buttons_frame, text="测试API", command=self.test_current_api)
            self.test_api_btn.pack(side="left", padx=2)
        
        def show_step(self, step):
            """显示指定步骤"""
            self.current_step = step
            
            # 更新步骤指示器
            for i, label in enumerate(self.step_labels):
                if i == step:
                    label.configure(text_color="green", font=("微软雅黑", 12, "bold"))
                else:
                    label.configure(text_color="black", font=("微软雅黑", 12, "normal"))
            
            # 清空内容区域
            for widget in self.content_frame.winfo_children():
                widget.destroy()
            
            # 显示对应步骤内容
            if step == 0:
                self.show_api_step()
            elif step == 1:
                self.show_preview_step()
            elif step == 2:
                self.show_email_step()
            
            # 更新按钮状态
            self.update_buttons()
        
        def show_api_step(self):
            """显示API配置步骤"""
            CTkLabel(self.content_frame, text="API配置", font=("微软雅黑", 14, "bold")).pack(anchor="w", pady=10)
            
            # 任务名称
            CTkLabel(self.content_frame, text="任务名称:").pack(anchor="w", pady=5)
            self.task_name_entry = CTkEntry(self.content_frame, width=500)
            self.task_name_entry.insert(0, self.task_config["name"])
            self.task_name_entry.pack(anchor="w", pady=5)

            # API配置区域
            self.api_configs_frame = CTkFrame(self.content_frame)
            self.api_configs_frame.pack(fill="x", pady=10)
            
            # API配置标签页
            self.api_tabview = CTkTabview(self.api_configs_frame)
            self.api_tabview.pack(fill="both", expand=True, padx=10, pady=10)
            
            # 存储API配置控件
            self.api_config_widgets = {}
            
            # 添加API按钮
            
            # 初始化API配置
            self.init_api_configs()
        
        def init_api_configs(self):
            """初始化API配置"""
            api_configs = self.task_config.get("api_configs", [])
            
            for i, api_config in enumerate(api_configs):
                api_name = api_config.get("name", f"API{i+1}")
                self.add_api_tab(api_name, api_config, i)
        
        def add_api_config(self):
            """添加新的API配置"""
            api_count = len(self.api_config_widgets)
            api_name = f"API{api_count + 1}"
            
            # 创建默认API配置
            new_api_config = {
                "name": api_name,
                "url": "",
                "headers": {
                    "appKey": "",
                    "appSecret": ""
                },
                "timeout": 30,
                "verify_ssl": True,
                "proxy": None
            }
            
            # 添加到任务配置
            if "api_configs" not in self.task_config:
                self.task_config["api_configs"] = []
            self.task_config["api_configs"].append(new_api_config)
            
            # 添加标签页
            self.add_api_tab(api_name, new_api_config, api_count)
            
            # 更新按钮状态
            self.update_api_buttons()
        
        def delete_current_api(self):
            """删除当前选中的API"""
            current_tab = self.api_tabview.get()
            if not current_tab:
                CTkMessagebox(title="提示", message="请先选择要删除的API标签页", icon="warning")
                return
            
            # API1不允许删除
            if current_tab == "API1":
                CTkMessagebox(title="提示", message="API1是默认API，不允许删除", icon="warning")
                return
            
            # 确认删除
            msg = CTkMessagebox(title="确认删除", message=f"确定要删除 {current_tab} 吗？", icon="question",
                              option_1="否", option_2="是")
            if msg.get() != "是":
                return
            
            # 从任务配置中删除
            api_configs = self.task_config.get("api_configs", [])
            for i, config in enumerate(api_configs):
                if config.get("name") == current_tab:
                    del api_configs[i]
                    break
            
            # 从界面中删除
            self.api_tabview.delete(current_tab)
            if current_tab in self.api_config_widgets:
                del self.api_config_widgets[current_tab]
            
            # 重新编号剩余的API
            self.renumber_apis()
            
            # 更新按钮状态
            self.update_api_buttons()
        
        def renumber_apis(self):
            """重新编号API名称"""
            api_configs = self.task_config.get("api_configs", [])
            for i, config in enumerate(api_configs):
                config["name"] = f"API{i+1}"
            
            # 重新构建界面
            self.rebuild_api_tabs()
        
        def rebuild_api_tabs(self):
            """重新构建API标签页"""
            # 清除现有标签页
            for widget in self.api_configs_frame.winfo_children():
                widget.destroy()
            
            # 重新创建标签页
            self.api_tabview = CTkTabview(self.api_configs_frame)
            self.api_tabview.pack(fill="both", expand=True, padx=10, pady=10)
            self.api_config_widgets.clear()
            
            # 重新添加API配置
            for i, api_config in enumerate(self.task_config.get("api_configs", [])):
                api_name = api_config.get("name", f"API{i+1}")
                self.add_api_tab(api_name, api_config, i)
        
        def test_current_api(self):
            """测试当前选中的API"""
            current_tab = self.api_tabview.get()
            if not current_tab:
                CTkMessagebox(title="提示", message="请先选择要测试的API标签页", icon="warning")
                return
            
            # 查找对应的API配置
            api_config = None
            for config in self.task_config.get("api_configs", []):
                if config.get("name") == current_tab:
                    api_config = config
                    break
            
            if not api_config:
                CTkMessagebox(title="错误", message=f"未找到 {current_tab} 的配置", icon="cancel")
                return
            
            # 保存当前API配置
            if current_tab in self.api_config_widgets:
                widgets = self.api_config_widgets[current_tab]
                url = widgets["url_entry"].get()
                headers = {}
                for key_entry, value_entry, _ in widgets["headers_entries"]:
                    key = key_entry.get().strip()
                    value = value_entry.get().strip()
                    if key and value:
                        headers[key] = value
                
                # 临时更新配置进行测试
                api_config["url"] = url
                api_config["headers"] = headers
            
            try:
                # 不使用缓存进行API测试
                df = fetch_api_data(self.task_config, current_tab, use_cache=False)
                if df is not None:
                    CTkMessagebox(title="测试成功", message=f"API {current_tab} 连接成功，获取到 {len(df)} 行数据", icon="check")
                else:
                    CTkMessagebox(title="测试失败", message=f"API {current_tab} 连接失败，请检查配置", icon="cancel")
            except Exception as e:
                CTkMessagebox(title="测试失败", message=f"API {current_tab} 测试错误: {e}", icon="cancel")
        
        def update_api_buttons(self):
            """更新API按钮状态"""
            api_count = len(self.api_config_widgets)
            
            # 删除按钮状态
            if api_count <= 1:
                self.delete_api_btn.configure(state="disabled")
            else:
                self.delete_api_btn.configure(state="normal")
        
        def add_api_tab(self, api_name, api_config, index):
            """添加API标签页"""
            # 添加标签页
            self.api_tabview.add(api_name)
            
            tab = self.api_tabview.tab(api_name)
            tab.grid_columnconfigure(0, weight=1)
            
            # URL配置
            CTkLabel(tab, text="API地址:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
            url_entry = CTkEntry(tab, width=400)
            url_entry.insert(0, api_config.get("url", ""))
            url_entry.grid(row=1, column=0, padx=5, pady=5, sticky="w")
            
            # Headers配置
            CTkLabel(tab, text="请求头配置:").grid(row=2, column=0, sticky="w", padx=5, pady=5)
            
            headers_frame = CTkFrame(tab)
            headers_frame.grid(row=3, column=0, padx=5, pady=5, sticky="w")
            headers_frame.grid_columnconfigure(1, weight=1)
            
            headers_entries = []
            headers = api_config.get("headers", {})
            for i, (key, value) in enumerate(headers.items()):
                self.add_header_row_to_api(headers_frame, key, value, i, headers_entries)
            
            # 添加header按钮
            add_header_btn = CTkButton(headers_frame, text="添加Header",
                                     command=lambda f=headers_frame, e=headers_entries: self.add_header_row_to_api(f, "", "", len(e), e))
            add_header_btn.grid(row=len(headers_entries), column=0, columnspan=3, pady=5)
            
            # 移除了单个API的测试连接按钮，统一使用底部的测试API按钮
            
            # 存储控件引用
            self.api_config_widgets[api_name] = {
                "url_entry": url_entry,
                "headers_entries": headers_entries,
                "headers_frame": headers_frame,
                "tab": tab
            }
        
        def add_header_row_to_api(self, headers_frame, key, value, row, headers_entries):
            """为指定API添加header行"""
            # Key输入框
            key_entry = CTkEntry(headers_frame, width=150, placeholder_text="Header名称")
            key_entry.insert(0, key)
            key_entry.grid(row=row, column=0, padx=5, pady=2)
            
            # Value输入框
            value_entry = CTkEntry(headers_frame, width=200, placeholder_text="Header值")
            value_entry.insert(0, value)
            value_entry.grid(row=row, column=1, padx=5, pady=2)
            
            # 删除按钮
            del_btn = CTkButton(headers_frame, text="删除", width=60,
                             command=lambda: self.remove_header_row_from_api(headers_frame, row, headers_entries))
            del_btn.grid(row=row, column=2, padx=5, pady=2)
            
            headers_entries.append((key_entry, value_entry, del_btn))
        
        def remove_header_row_from_api(self, headers_frame, row, headers_entries):
            """从指定API删除header行"""
            if row < len(headers_entries):
                for widget in headers_entries[row]:
                    widget.destroy()
                headers_entries.pop(row)
        
        def test_single_api(self, api_name, api_config):
            """测试单个API连接"""
            # 保存当前API配置
            url = self.api_config_widgets[api_name]["url_entry"].get()
            headers = {}
            for key_entry, value_entry, _ in self.api_config_widgets[api_name]["headers_entries"]:
                key = key_entry.get().strip()
                value = value_entry.get().strip()
                if key and value:
                    headers[key] = value
            
            # 临时更新配置进行测试
            api_config["url"] = url
            api_config["headers"] = headers
            
            try:
                # 不使用缓存进行API测试
                df = fetch_api_data(self.task_config, api_name, use_cache=False)
                if df is not None:
                    CTkMessagebox(title="测试成功", message=f"API {api_name} 连接成功，获取到 {len(df)} 行数据", icon="check")
                else:
                    CTkMessagebox(title="测试失败", message=f"API {api_name} 连接失败，请检查配置", icon="cancel")
            except Exception as e:
                CTkMessagebox(title="测试失败", message=f"API {api_name} 测试错误: {e}", icon="cancel")
        
        def add_header_row(self, key="", value="", row=None):
            """添加header行"""
            if row is None:
                row = len(self.headers_entries)
            
            # Key输入框
            key_entry = CTkEntry(self.headers_frame, width=150, placeholder_text="Header名称")
            key_entry.insert(0, key)
            key_entry.grid(row=row, column=0, padx=5, pady=2)
            
            # Value输入框
            value_entry = CTkEntry(self.headers_frame, width=300, placeholder_text="Header值")
            value_entry.insert(0, value)
            value_entry.grid(row=row, column=1, padx=5, pady=2)
            
            # 删除按钮
            del_btn = CTkButton(self.headers_frame, text="删除", width=60, command=lambda: self.remove_header_row(row))
            del_btn.grid(row=row, column=2, padx=5, pady=2)
            
            self.headers_entries.append((key_entry, value_entry, del_btn))
        
        def remove_header_row(self, row):
            """删除header行"""
            if row < len(self.headers_entries):
                for widget in self.headers_entries[row]:
                    widget.destroy()
                self.headers_entries.pop(row)
        
        def show_preview_step(self):
            """显示数据预览步骤"""
            # 整体框架
            self.preview_main_frame = CTkFrame(self.content_frame)
            self.preview_main_frame.pack(fill="both", expand=True)
            self.preview_main_frame.grid_columnconfigure(0, weight=1)
            self.preview_main_frame.grid_rowconfigure(1, weight=1)

            # 顶部配置区
            config_frame = CTkFrame(self.preview_main_frame, fg_color="transparent")
            config_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=10)
            config_frame.grid_columnconfigure(1, weight=1)

            CTkLabel(config_frame, text="数据预览", font=("微软雅黑", 14, "bold")).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 10))

            # Excel文件名配置
            CTkLabel(config_frame, text="excel文件名:").grid(row=1, column=0, sticky="w", padx=5)
            self.filename_entry = CTkEntry(config_frame)
            self.filename_entry.insert(0, self.task_config["data_config"].get("filename_pattern", "{taskName}_{date}.xlsx"))
            self.filename_entry.grid(row=1, column=1, sticky="ew", padx=5)

            # Sheet名称配置
            self.sheet_name_entries = []
            api_configs = self.task_config.get("api_configs", [])
            sheet_count = len(api_configs) if api_configs else 1
            existing_sheet_names = self.task_config["data_config"].get("sheet_names", [])

            for i in range(sheet_count):
                default_name = existing_sheet_names[i] if i < len(existing_sheet_names) else f"Sheet{i+1}"
                row = i + 2
                label_text = f"Sheet{i+1}:"
                
                CTkLabel(config_frame, text=label_text).grid(row=row, column=0, sticky="w", padx=5, pady=2)
                sheet_entry = CTkEntry(config_frame)
                sheet_entry.insert(0, default_name)
                sheet_entry.grid(row=row, column=1, sticky="ew", padx=5, pady=2)
                self.sheet_name_entries.append(sheet_entry) # 只存储输入框

            # 中间数据预览区
            self.preview_display_frame = CTkFrame(self.preview_main_frame)
            self.preview_display_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=10)
            self.preview_display_frame.grid_columnconfigure(0, weight=1)
            self.preview_display_frame.grid_rowconfigure(0, weight=1)
            
            self.sheet_tabview = CTkTabview(self.preview_display_frame)
            self.sheet_tabview.pack(fill="both", expand=True)
            
            # 底部按钮区由 self.update_buttons() 统一管理
        
        def add_sheet_name_row(self, sheet_name="", row=None):
            """添加Sheet名称行"""
            if row is None:
                row = len(self.sheet_name_entries)
            
            # Sheet名称输入框
            sheet_entry = CTkEntry(self.sheet_names_frame, width=150, placeholder_text="Sheet名称")
            sheet_entry.insert(0, sheet_name)
            sheet_entry.grid(row=row, column=0, padx=5, pady=2)
            
            # 删除按钮
            del_btn = CTkButton(self.sheet_names_frame, text="删除", width=60,
                             command=lambda: self.remove_sheet_name_row(row))
            del_btn.grid(row=row, column=1, padx=5, pady=2)
            
            self.sheet_name_entries.append((sheet_entry, del_btn))
        
        def add_sheet_name_row_with_label(self, label_text, sheet_name="", row=None):
            """添加Sheet名称行（带标签的版本）"""
            if row is None:
                row = len(self.sheet_name_entries)
            
            # Sheet标签
            sheet_label = CTkLabel(self.sheet_names_frame, text=label_text, font=("微软雅黑", 9))
            sheet_label.grid(row=row, column=0, padx=5, pady=2, sticky="w")
            
            # Sheet名称输入框
            sheet_entry = CTkEntry(self.sheet_names_frame, width=150, placeholder_text="Sheet名称")
            sheet_entry.insert(0, sheet_name)
            sheet_entry.grid(row=row, column=1, padx=5, pady=2)
            
            # 删除按钮
            del_btn = CTkButton(self.sheet_names_frame, text="删除", width=60,
                             command=lambda: self.remove_sheet_name_row_with_label(row))
            del_btn.grid(row=row, column=2, padx=5, pady=2)
            
            self.sheet_name_entries.append((sheet_label, sheet_entry, del_btn))


        
        def show_email_step(self):
            """显示邮箱配置步骤"""
            CTkLabel(self.content_frame, text="邮箱配置", font=("微软雅黑", 14, "bold")).pack(anchor="w", pady=10)
            
            # 发件人配置（紧凑布局）
            sender_frame = CTkFrame(self.content_frame)
            sender_frame.pack(fill="x", pady=5)
            
            CTkLabel(sender_frame, text="发件人邮箱:").grid(row=0, column=0, sticky="w", padx=5, pady=2)
            self.sender_entry = CTkEntry(sender_frame, width=300)
            self.sender_entry.insert(0, self.task_config["email_config"]["sender"]["email"])
            self.sender_entry.grid(row=0, column=1, padx=5, pady=2)
            
            CTkLabel(sender_frame, text="发件人密码:").grid(row=1, column=0, sticky="w", padx=5, pady=2)
            self.password_entry = CTkEntry(sender_frame, width=300, show="*")
            self.password_entry.grid(row=1, column=1, padx=5, pady=2)
            
            # 收件人配置（紧凑布局）
            recipients_frame = CTkFrame(self.content_frame)
            recipients_frame.pack(fill="x", pady=5)
            
            # 收件人和抄送人放在同一行
            CTkLabel(recipients_frame, text="收件人 (逗号分隔):").grid(row=0, column=0, sticky="w", padx=5, pady=2)
            self.to_entry = CTkEntry(recipients_frame, width=300)
            self.to_entry.insert(0, ",".join(self.task_config["email_config"]["recipients"]["to"]))
            self.to_entry.grid(row=0, column=1, padx=5, pady=2)
            
            CTkLabel(recipients_frame, text="抄送人 (逗号分隔):").grid(row=1, column=0, sticky="w", padx=5, pady=2)
            self.cc_entry = CTkEntry(recipients_frame, width=300)
            self.cc_entry.insert(0, ",".join(self.task_config["email_config"]["recipients"]["cc"]))
            self.cc_entry.grid(row=1, column=1, padx=5, pady=2)
            
            # 邮件内容配置（紧凑布局）
            email_content_frame = CTkFrame(self.content_frame)
            email_content_frame.pack(fill="x", pady=5)
            
            CTkLabel(email_content_frame, text="邮件主题:").grid(row=0, column=0, sticky="w", padx=5, pady=2)
            self.subject_entry = CTkEntry(email_content_frame, width=300)
            self.subject_entry.insert(0, self.task_config["email_config"]["subject"])
            self.subject_entry.grid(row=0, column=1, padx=5, pady=2)
            
            # 邮件正文配置
            email_body_frame = CTkFrame(self.content_frame)
            email_body_frame.pack(fill="x", pady=5)
            
            # 邮件正文标题和帮助信息
            body_header_frame = CTkFrame(email_body_frame)
            body_header_frame.pack(fill="x", pady=5)
            
            CTkLabel(body_header_frame, text="邮件正文 (HTML):", font=("微软雅黑", 10, "bold")).pack(anchor="w")
            CTkLabel(body_header_frame, text="提示：正文中若包含 {Sheet1} 等变量，发送时会自动替换为对应数据表格",
                     font=("微软雅黑", 9), text_color="blue").pack(anchor="w")
            
            # 邮件正文编辑区域
            self.body_text = CTkTextbox(email_body_frame, width=300, height=100)
            self.body_text.insert("1.0", self.task_config["email_config"]["body"])
            self.body_text.pack(fill="x", padx=5, pady=5)
        
        def update_buttons(self):
            """统一更新所有步骤的底部按钮状态"""
            # 清空底部按钮栏
            for widget in self.button_frame.winfo_children():
                widget.pack_forget()

            # 根据当前步骤重建按钮
            if self.current_step == 0:
                # API配置步骤
                self.api_buttons_frame.pack(side="left", padx=5)
                self.next_btn = CTkButton(self.button_frame, text="下一步", command=self.next_step)
                self.next_btn.pack(side="right", padx=5)
            
            elif self.current_step == 1:
                # 数据预览步骤
                self.prev_btn = CTkButton(self.button_frame, text="上一步", command=self.prev_step)
                self.prev_btn.pack(side="left", padx=5)
                
                preview_btn = CTkButton(self.button_frame, text="获取数据预览", command=self.preview_data)
                preview_btn.pack(side="left", padx=5)

                self.download_btn = CTkButton(self.button_frame, text="下载数据", command=self.download_preview_data, state="disabled")
                self.download_btn.pack(side="left", padx=5)

                self.next_btn = CTkButton(self.button_frame, text="下一步", command=self.next_step)
                self.next_btn.pack(side="right", padx=5)

            elif self.current_step == 2:
                # 邮箱配置步骤
                self.prev_btn = CTkButton(self.button_frame, text="上一步", command=self.prev_step)
                self.prev_btn.pack(side="left", padx=5)

                self.test_run_btn = CTkButton(self.button_frame, text="测试运行", command=self.test_run)
                self.test_run_btn.pack(side="left", padx=5)
                
                self.save_btn = CTkButton(self.button_frame, text="保存", command=self.save_task, fg_color="green")
                self.save_btn.pack(side="right", padx=5)
        
        def prev_step(self):
            """上一步"""
            if self.current_step > 0:
                self.save_current_step()
                self.show_step(self.current_step - 1)
        
        def next_step(self):
            """下一步"""
            if self.current_step < len(self.steps) - 1:
                self.save_current_step()
                self.show_step(self.current_step + 1)
        
        def go_to_step(self, step_index):
            """跳转到指定步骤"""
            if step_index != self.current_step:
                self.save_current_step()
                self.show_step(step_index)

        def save_current_step(self):
            """保存当前步骤的数据"""
            if self.current_step == 0:
                # 保存任务名称
                self.task_config["name"] = self.task_name_entry.get()
                
                # 保存API配置
                if "api_configs" not in self.task_config:
                    self.task_config["api_configs"] = []
                
                # 更新每个API配置
                for api_name, widgets in self.api_config_widgets.items():
                    # 查找对应的API配置
                    api_config = None
                    for config in self.task_config["api_configs"]:
                        if config.get("name") == api_name:
                            api_config = config
                            break
                    
                    if api_config:
                        # 更新URL
                        api_config["url"] = widgets["url_entry"].get()
                        
                        # 更新Headers
                        headers = {}
                        for key_entry, value_entry, _ in widgets["headers_entries"]:
                            key = key_entry.get().strip()
                            value = value_entry.get().strip()
                            if key and value:
                                headers[key] = value
                        api_config["headers"] = headers
            
            elif self.current_step == 1:
                # 保存数据配置
                self.task_config["data_config"]["filename_pattern"] = self.filename_entry.get()
                
                # 保存Sheet名称配置
                sheet_names = [entry.get().strip() for entry in self.sheet_name_entries if entry.get().strip()]
                
                if not sheet_names:  # 如果没有配置Sheet名称，使用默认名称
                    sheet_names = ["Sheet1"]
                
                self.task_config["data_config"]["sheet_names"] = sheet_names
            
            elif self.current_step == 2:
                # 保存邮箱配置
                self.task_config["email_config"]["sender"]["email"] = self.sender_entry.get()
                
                password = self.password_entry.get()
                if password:
                    self.task_config["email_config"]["sender"]["password"] = password
                
                to_list = [email.strip() for email in self.to_entry.get().split(",") if email.strip()]
                cc_list = [email.strip() for email in self.cc_entry.get().split(",") if email.strip()]
                
                self.task_config["email_config"]["recipients"]["to"] = to_list
                self.task_config["email_config"]["recipients"]["cc"] = cc_list
                self.task_config["email_config"]["subject"] = self.subject_entry.get()
                self.task_config["email_config"]["body"] = self.body_text.get("1.0", "end").strip()
        
        def load_current_step(self):
            """加载当前步骤的数据"""
            if self.current_step == 0:
                # 加载任务名称
                if "name" in self.task_config:
                    self.task_name_entry.delete(0, "end")
                    self.task_name_entry.insert(0, self.task_config["name"])
                
                # 加载API配置
                if "api_configs" in self.task_config:
                    for api_config in self.task_config["api_configs"]:
                        api_name = api_config.get("name", "API")
                        # self.add_api_config(api_name) # This was causing the error
                        
                        # 填充API配置
                        if api_name in self.api_config_widgets:
                            widgets = self.api_config_widgets[api_name]
                            widgets["url_entry"].delete(0, "end")
                            if "url" in api_config:
                                widgets["url_entry"].insert(0, api_config["url"])
                            
                            # 清空现有的Headers
                            for _, _, remove_btn in widgets["headers_entries"]:
                                remove_btn.destroy()
                            widgets["headers_entries"].clear()
                            
                            # 添加Headers
                            if "headers" in api_config:
                                for key, value in api_config["headers"].items():
                                    self.add_header_row_to_api(widgets["headers_frame"], key, value, len(widgets["headers_entries"]), widgets["headers_entries"])
            
            elif self.current_step == 1:
                # 加载数据配置
                if "data_config" in self.task_config:
                    if "filename_pattern" in self.task_config["data_config"]:
                        self.filename_entry.delete(0, "end")
                        self.filename_entry.insert(0, self.task_config["data_config"]["filename_pattern"])
                    
                    # 加载Sheet名称配置
                    if "sheet_names" in self.task_config["data_config"] and self.sheet_name_entries:
                        sheet_names = self.task_config["data_config"]["sheet_names"]
                        for i, entry in enumerate(self.sheet_name_entries):
                            if i < len(sheet_names):
                                entry.delete(0, "end")
                                entry.insert(0, sheet_names[i])
            
            elif self.current_step == 2:
                # 加载邮箱配置
                if "email_config" in self.task_config:
                    email_config = self.task_config["email_config"]
                    if "sender" in email_config:
                        sender = email_config["sender"]
                        if "email" in sender:
                            self.sender_entry.delete(0, "end")
                            self.sender_entry.insert(0, sender["email"])
                        if "password" in sender:
                            self.password_entry.delete(0, "end")
                            self.password_entry.insert(0, sender["password"])
                    
                    if "recipients" in email_config:
                        recipients = email_config["recipients"]
                        if "to" in recipients:
                            self.to_entry.delete(0, "end")
                            self.to_entry.insert(0, ", ".join(recipients["to"]))
                        if "cc" in recipients:
                            self.cc_entry.delete(0, "end")
                            self.cc_entry.insert(0, ", ".join(recipients["cc"]))
                    
                    if "subject" in email_config:
                        self.subject_entry.delete(0, "end")
                        self.subject_entry.insert(0, email_config["subject"])
                    
                    if "body" in email_config:
                        self.body_text.delete("1.0", "end")
                        self.body_text.insert("1.0", email_config["body"])
        
        def test_api(self):
            """测试API连接"""
            self.save_current_step()
            try:
                # 不使用缓存进行API测试，确保每次都真实测试
                df = fetch_api_data(self.task_config, use_cache=False)
                if df is not None:
                    CTkMessagebox(title="测试成功", message=f"API连接成功，获取到 {len(df)} 行数据", icon="check")
                else:
                    CTkMessagebox(title="测试失败", message="API连接失败，请检查配置", icon="cancel")
            except Exception as e:
                CTkMessagebox(title="测试失败", message=f"API测试错误: {e}", icon="cancel")
        
        def preview_data(self):
            """预览数据"""
            self.save_current_step()
            try:
                # 使用缓存获取所有API数据
                data_frames = fetch_all_api_data(self.task_config, use_cache=True)
                if data_frames and any(df is not None for df in data_frames.values()):
                    # 清空预览区域
                    # 清空旧的标签页
                    for tab_name in self.sheet_tabview._name_list:
                        self.sheet_tabview.delete(tab_name)
                    
                    # 获取Sheet名称配置
                    sheet_names = [entry.get().strip() for entry in self.sheet_name_entries if entry.get().strip()]
                    
                    # 如果没有配置Sheet名称，使用默认名称
                    if not sheet_names:
                        sheet_names = ["Sheet1"]
                    
                    # 为每个API创建Sheet标签页
                    for i, (api_name, df) in enumerate(data_frames.items()):
                        if df is not None:
                            # 获取对应的Sheet名称
                            sheet_name = sheet_names[i] if i < len(sheet_names) else f"Sheet{i+1}"
                            
                            # 添加标签页
                            self.sheet_tabview.add(sheet_name)
                            
                            tab = self.sheet_tabview.tab(sheet_name)
                            tab.grid_columnconfigure(0, weight=1)
                            tab.grid_rowconfigure(0, weight=1)
                            
                            # 创建可滚动的表格框架
                            table_frame = CTkScrollableFrame(tab)
                            table_frame.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")
                            
                            # 获取列名和数据（显示前10行）
                            headers = df.columns.tolist()
                            data = df.head(10).values.tolist()
                            
                            # 创建表头
                            for col_idx, header in enumerate(headers):
                                header_label = CTkLabel(table_frame, text=header, font=("微软雅黑", 10, "bold"))
                                header_label.grid(row=0, column=col_idx, padx=5, pady=2, sticky="w")
                            
                            # 填充数据行
                            for row_idx, row_data in enumerate(data, start=1):
                                for col_idx, cell_data in enumerate(row_data):
                                    cell_label = CTkLabel(table_frame, text=str(cell_data), font=("微软雅黑", 10))
                                    cell_label.grid(row=row_idx, column=col_idx, padx=5, pady=2, sticky="w")
                            
                            # 显示数据统计
                            stats_label = CTkLabel(tab, text=f"API: {api_name} | 共 {len(df)} 行数据，显示前10行",
                                                font=("微软雅黑", 9))
                            stats_label.grid(row=1, column=0, padx=10, pady=5, sticky="w")
                    
                    self.download_btn.configure(state="normal") # 启用下载按钮
                else:
                    CTkMessagebox(title="预览失败", message="数据获取失败或所有API都返回空数据", icon="cancel")
                    self.download_btn.configure(state="disabled") # 禁用下载按钮
            except Exception as e:
                CTkMessagebox(title="预览失败", message=f"数据预览错误: {e}", icon="cancel")
                self.download_btn.configure(state="disabled") # 禁用下载按钮
        
        def download_preview_data(self):
            """下载预览的Excel数据"""
            try:
                # 获取所有API数据
                data_frames = fetch_all_api_data(self.task_config, use_cache=True)
                if data_frames and any(df is not None for df in data_frames.values()):
                    from tkinter import filedialog
                    file_path = filedialog.asksaveasfilename(
                        defaultextension=".xlsx",
                        filetypes=[("Excel 文件", "*.xlsx"), ("所有文件", "*.*")],
                        title="保存Excel文件"
                    )
                    if file_path:
                        # 生成包含多个Sheet的Excel文件
                        success = generate_excel_file_with_sheets(self.task_config, data_frames)
                        if success:
                            # 移动临时文件到指定位置
                            import shutil
                            temp_file = success
                            shutil.move(temp_file, file_path)
                            CTkMessagebox(title="下载成功", message=f"数据已保存到:\n{file_path}", icon="check")
                        else:
                            CTkMessagebox(title="下载失败", message="Excel文件生成失败", icon="cancel")
                else:
                    CTkMessagebox(title="下载失败", message="没有数据可下载", icon="cancel")
            except Exception as e:
                CTkMessagebox(title="下载失败", message=f"文件保存失败: {e}", icon="cancel")
        
        def test_run(self):
            """测试运行"""
            self.save_current_step()
            if not self.task_config["name"]:
                CTkMessagebox(title="警告", message="请先输入任务名称", icon="warning")
                return
            
            try:
                # 测试运行前先获取数据并缓存
                df = fetch_api_data(self.task_config, use_cache=True)
                if df is None:
                    CTkMessagebox(title="测试失败", message="数据获取失败，无法进行测试运行", icon="cancel")
                    return
                
                # 然后执行任务
                success = execute_task(self.task_config["name"])
                if success:
                    CTkMessagebox(title="测试成功", message="任务执行成功！", icon="check")
                else:
                    CTkMessagebox(title="测试失败", message="任务执行失败，请查看日志", icon="cancel")
            except Exception as e:
                CTkMessagebox(title="测试失败", message=f"测试运行错误: {e}", icon="cancel")
        
        def save_task(self):
            """保存任务"""
            self.save_current_step()
            if not self.task_config["name"]:
                CTkMessagebox(title="警告", message="请输入任务名称", icon="warning")
                return
            
            try:
                add_task_config(self.task_config)
                self.parent.refresh_task_list()
                CTkMessagebox(title="保存成功", message="任务配置已保存", icon="check")
                self.after(100, self.destroy) # 延迟销毁窗口
            except Exception as e:
                CTkMessagebox(title="保存失败", message=f"保存配置失败: {e}", icon="cancel")


    class TaskManagerApp(CTk):
        """任务管理主窗口"""
        def __init__(self):
            super().__init__()
            self.title("百川数据助手")
            self.geometry("1000x700")
            self.resizable(True, True)
            
            # 设置主题
            ctk.set_appearance_mode("light")
            ctk.set_default_color_theme("blue")
            
            self.setup_ui()
            self.refresh_task_list()
        
        def setup_ui(self):
            """设置主界面"""
            # 任务列表区域
            self.task_list_frame = CTkFrame(self)
            self.task_list_frame.pack(fill="both", expand=True, padx=20, pady=10)

            # 创建滚动框架
            self.scrollable_frame = CTkScrollableFrame(self.task_list_frame)
            self.scrollable_frame.pack(fill="both", expand=True)

            # 底部提示信息
            warning_label = CTkLabel(
                self,
                text="⚠️ 重要提示：本工具仅针对江苏电信百川平台API开发，使用前请确认是否有平台访问权限",
                font=("微软雅黑", 12, "bold"),
                text_color="red"
            )
            warning_label.pack(side="bottom", fill="x", padx=20, pady=10)

            # 底部按钮栏
            button_frame = CTkFrame(self)
            button_frame.pack(side="bottom", fill="x", padx=20, pady=10)

            # 操作按钮（默认禁用）
            self.new_task_btn = CTkButton(button_frame, text="新建任务", command=self.new_task, fg_color="green")
            self.new_task_btn.pack(side="left", padx=5)

            self.edit_btn = CTkButton(button_frame, text="编辑", command=self.edit_selected_task, state="disabled")
            self.edit_btn.pack(side="left", padx=5)

            self.test_btn = CTkButton(button_frame, text="测试运行", command=self.test_selected_task, state="disabled")
            self.test_btn.pack(side="left", padx=5)

            self.schedule_btn = CTkButton(button_frame, text="定时", command=self.toggle_selected_schedule, state="disabled")
            self.schedule_btn.pack(side="left", padx=5)

            self.delete_btn = CTkButton(button_frame, text="删除", command=self.delete_selected_task, fg_color="red", state="disabled")
            self.delete_btn.pack(side="left", padx=5)

            # 刷新按钮
            refresh_btn = CTkButton(button_frame, text="刷新", command=self.refresh_task_list)
            refresh_btn.pack(side="right", padx=5)

            # 存储当前选中的任务
            self.selected_task = None
        
        def refresh_task_list(self):
            """刷新任务列表"""
            # 清空现有任务卡片
            for widget in self.scrollable_frame.winfo_children():
                widget.destroy()
            
            # 清除复选框状态
            self.task_checkboxes = {}
            self.selected_task = None
            
            # 禁用所有操作按钮
            self.edit_btn.configure(state="disabled")
            self.test_btn.configure(state="disabled")
            self.schedule_btn.configure(state="disabled")
            self.delete_btn.configure(state="disabled")
            
            # 获取任务列表
            config = load_config()
            tasks = config.get("tasks", [])
            
            if not tasks:
                # 显示空状态
                empty_label = CTkLabel(self.scrollable_frame, text="暂无任务，请点击'新建任务'开始配置", font=("微软雅黑", 12))
                empty_label.pack(expand=True)
                return
            
            # 显示任务卡片
            for task in tasks:
                self.create_task_card(task)
        
        def create_task_card(self, task):
            """创建任务卡片"""
            card_frame = CTkFrame(self.scrollable_frame, border_width=1, border_color="gray")
            card_frame.pack(fill="x", padx=10, pady=5)
            
            card_frame.grid_columnconfigure(1, weight=1)

            # 复选框
            checkbox_var = ctk.BooleanVar()
            checkbox = CTkCheckBox(card_frame, text="", variable=checkbox_var,
                                 command=lambda t=task, v=checkbox_var: self.on_task_select(t, v))
            checkbox.grid(row=0, column=0, padx=5, pady=5, sticky="w")
            
            # 任务基本信息
            info_frame = CTkFrame(card_frame)
            info_frame.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
            
            # 任务名称
            name_label = CTkLabel(info_frame, text=f"任务名称: {task['name']}", font=("微软雅黑", 12, "bold"))
            name_label.grid(row=0, column=0, sticky="w", padx=5, pady=2)
            
            # API配置信息（支持多API）
            api_configs = task.get("api_configs", [])
            if api_configs:
                api_info = []
                for api_config in api_configs:
                    api_name = api_config.get("name", "API")
                    api_url = api_config.get("url", "")
                    domain = api_url.split("//")[-1].split("/")[0] if "//" in api_url else api_url
                    api_info.append(f"{api_name}: {domain}")
                api_text = " | ".join(api_info)
            else:
                api_text = "未配置API"
            CTkLabel(info_frame, text=f"API配置: {api_text}").grid(row=1, column=0, sticky="w", padx=5, pady=2)
            
            # 收件人数量
            to_count = len(task["email_config"]["recipients"]["to"])
            cc_count = len(task["email_config"]["recipients"]["cc"])
            CTkLabel(info_frame, text=f"收件人: {to_count}人, 抄送: {cc_count}人").grid(row=2, column=0, sticky="w", padx=5, pady=2)
            
            # Sheet配置信息
            sheet_names = task["data_config"].get("sheet_names", ["Sheet1"])
            sheet_text = f"Sheet: {', '.join(sheet_names)}"
            CTkLabel(info_frame, text=sheet_text).grid(row=3, column=0, sticky="w", padx=5, pady=2)
            
            # 定时任务状态显示（替代原来的状态显示）
            schedule_enabled = task["schedule_config"]["enabled"]
            schedule_status_text = "定时: 启用" if schedule_enabled else "定时: 未启用"
            schedule_status_color = "orange" if schedule_enabled else "gray"
            schedule_status_label = CTkLabel(info_frame, text=schedule_status_text, text_color=schedule_status_color)
            schedule_status_label.grid(row=0, column=1, sticky="e", padx=5, pady=2)
            
            # 存储任务和复选框变量的引用
            if not hasattr(self, 'task_checkboxes'):
                self.task_checkboxes = {}
            self.task_checkboxes[task["name"]] = {
                'task': task,
                'checkbox_var': checkbox_var,
                'checkbox': checkbox,
                'schedule_status_label': schedule_status_label
            }
        
        def on_task_select(self, task, checkbox_var):
            """处理任务选择"""
            if checkbox_var.get():
                # 如果选中，取消其他所有选中状态
                for task_name, data in self.task_checkboxes.items():
                    if task_name != task["name"]:
                        data['checkbox_var'].set(False)
                
                # 启用操作按钮
                self.selected_task = task
                self.edit_btn.configure(state="normal")
                self.test_btn.configure(state="normal")
                self.schedule_btn.configure(state="normal")
                self.delete_btn.configure(state="normal")
                
                # 更新定时按钮文本
                schedule_enabled = task["schedule_config"]["enabled"]
                schedule_text = "注销定时" if schedule_enabled else "注册定时"
                schedule_color = "orange" if schedule_enabled else "blue"
                self.schedule_btn.configure(text=schedule_text, fg_color=schedule_color)
            else:
                # 如果取消选中，禁用操作按钮
                self.selected_task = None
                self.edit_btn.configure(state="disabled")
                self.test_btn.configure(state="disabled")
                self.schedule_btn.configure(state="disabled")
                self.delete_btn.configure(state="disabled")
        
        def edit_selected_task(self):
            """编辑选中的任务"""
            if self.selected_task:
                self.edit_task(self.selected_task)
        
        def test_selected_task(self):
            """测试运行选中的任务"""
            if self.selected_task:
                self.test_task(self.selected_task)
        
        def toggle_selected_schedule(self):
            """切换选中任务的定时"""
            if self.selected_task:
                self.toggle_schedule(self.selected_task)
        
        def delete_selected_task(self):
            """删除选中的任务"""
            if self.selected_task:
                self.delete_task(self.selected_task)
        
        def new_task(self):
            """新建任务"""
            # 创建新任务配置
            new_task = TASK_TEMPLATE.copy()
            new_task["name"] = f"新任务_{len(load_config().get('tasks', [])) + 1}"
            
            # 打开配置向导
            wizard = TaskConfigWizard(self, new_task)
            wizard.transient(self)
            wizard.grab_set()
        
        def edit_task(self, task):
            """编辑任务"""
            # 创建任务配置副本
            task_copy = task.copy()
            task_copy["data_config"] = task["data_config"].copy()
            task_copy["email_config"] = task["email_config"].copy()
            task_copy["email_config"]["sender"] = task["email_config"]["sender"].copy()
            task_copy["recipients"] = task["email_config"]["recipients"].copy()
            
            # 打开配置向导
            wizard = TaskConfigWizard(self, task_copy)
            wizard.transient(self)
            wizard.grab_set()
        
        def test_task(self, task):
            """测试运行任务"""
            try:
                success = execute_task(task["name"])
                if success:
                    CTkMessagebox(title="测试成功", message=f"任务 '{task['name']}' 执行成功！", icon="check")
                else:
                    CTkMessagebox(title="测试失败", message=f"任务 '{task['name']}' 执行失败", icon="cancel")
            except Exception as e:
                CTkMessagebox(title="测试失败", message=f"测试运行错误: {e}", icon="cancel")
        
        def toggle_schedule(self, task):
            """切换定时任务（支持新建、启用、禁用、删除四种操作）"""
            task_name = task["name"]
            schedule_enabled = task["schedule_config"]["enabled"]
            
            try:
                if schedule_enabled:
                    # 任务已启用，提供禁用选项
                    msg = CTkMessagebox(title="定时任务操作",
                                      message=f"任务 '{task_name}' 已启用，请选择操作：",
                                      option_1="禁用", option_2="删除", option_3="取消")
                    choice = msg.get()
                    
                    if choice == "禁用":
                        # 禁用定时任务
                        success = disable_scheduled_task(task_name)
                        if success:
                            task["schedule_config"]["enabled"] = False
                            add_task_config(task)
                            CTkMessagebox(title="成功", message=f"已禁用任务 '{task_name}' 的定时计划", icon="check")
                        else:
                            CTkMessagebox(title="失败", message="禁用定时任务失败", icon="cancel")
                    elif choice == "删除":
                        # 删除定时任务
                        success = delete_scheduled_task(task_name)
                        if success:
                            task["schedule_config"]["enabled"] = False
                            add_task_config(task)
                            CTkMessagebox(title="成功", message=f"已删除任务 '{task_name}' 的定时计划", icon="check")
                        else:
                            CTkMessagebox(title="失败", message="删除定时任务失败", icon="cancel")
                    # 如果选择取消，不做任何操作
                else:
                    # 任务未启用，检查Windows中是否存在
                    status = get_task_status(task_name)
                    
                    if status == 'not_found':
                        # 任务不存在，创建新任务
                        self.show_schedule_config_dialog(task)
                    elif status == 'disabled':
                        # 任务已存在但被禁用，提供启用选项
                        msg = CTkMessagebox(title="定时任务操作",
                                          message=f"任务 '{task_name}' 在Windows中已存在但被禁用，是否启用？",
                                          option_1="启用", option_2="删除", option_3="取消")
                        choice = msg.get()
                        
                        if choice == "启用":
                            # 启用定时任务
                            success = enable_scheduled_task(task_name)
                            if success:
                                task["schedule_config"]["enabled"] = True
                                add_task_config(task)
                                CTkMessagebox(title="成功", message=f"已启用任务 '{task_name}' 的定时计划", icon="check")
                            else:
                                CTkMessagebox(title="失败", message="启用定时任务失败", icon="cancel")
                        elif choice == "删除":
                            # 删除定时任务
                            success = delete_scheduled_task(task_name)
                            if success:
                                CTkMessagebox(title="成功", message=f"已删除任务 '{task_name}' 的定时计划", icon="check")
                            else:
                                CTkMessagebox(title="失败", message="删除定时任务失败", icon="cancel")
                        # 如果选择取消，不做任何操作
                    else:
                        # 其他状态，直接创建新任务
                        self.show_schedule_config_dialog(task)
                
                # 无论成功与否都刷新列表，确保状态同步
                self.refresh_task_list()
            except Exception as e:
                CTkMessagebox(title="操作失败", message=f"定时任务操作错误: {e}", icon="cancel")
        
        def update_task_status_display(self, task_name, schedule_enabled):
            """更新指定任务的状态显示"""
            if hasattr(self, 'task_checkboxes') and task_name in self.task_checkboxes:
                data = self.task_checkboxes[task_name]
                schedule_status_text = "定时: 启用" if schedule_enabled else "定时: 未启用"
                schedule_status_color = "orange" if schedule_enabled else "gray"
                data['schedule_status_label'].configure(text=schedule_status_text, text_color=schedule_status_color)
                
                # 更新定时按钮文本
                if self.selected_task and self.selected_task["name"] == task_name:
                    if schedule_enabled:
                        self.schedule_btn.configure(text="管理定时", fg_color="orange")
                    else:
                        # 检查Windows中是否存在任务
                        status = get_task_status(task_name)
                        if status == 'not_found':
                            self.schedule_btn.configure(text="注册定时", fg_color="blue")
                        elif status == 'disabled':
                            self.schedule_btn.configure(text="管理定时", fg_color="orange")
                        else:
                            self.schedule_btn.configure(text="注册定时", fg_color="blue")
        
        def show_schedule_config_dialog(self, task):
            """显示定时任务配置弹窗"""
            dialog = CTkToplevel(self)
            dialog.title("定时任务配置")
            dialog.geometry("400x300")
            dialog.transient(self)
            dialog.grab_set()
            
            # 频率选择
            CTkLabel(dialog, text="执行频率:", font=("微软雅黑", 12, "bold")).pack(anchor="w", padx=20, pady=10)
            
            frequency_var = ctk.StringVar(value=task["schedule_config"].get("frequency", "DAILY"))
            frequency_frame = CTkFrame(dialog)
            frequency_frame.pack(fill="x", padx=20, pady=5)
            
            CTkRadioButton(frequency_frame, text="每天", variable=frequency_var, value="DAILY").pack(side="left", padx=5)
            CTkRadioButton(frequency_frame, text="每周", variable=frequency_var, value="WEEKLY").pack(side="left", padx=5)
            
            # 时间选择
            CTkLabel(dialog, text="执行时间:", font=("微软雅黑", 12, "bold")).pack(anchor="w", padx=20, pady=10)
            
            time_frame = CTkFrame(dialog)
            time_frame.pack(fill="x", padx=20, pady=5)
            
            hour_var = ctk.StringVar(value=task["schedule_config"].get("time", "18:00").split(":")[0])
            minute_var = ctk.StringVar(value=task["schedule_config"].get("time", "18:00").split(":")[1])
            
            CTkLabel(time_frame, text="时:").pack(side="left", padx=5)
            hour_combo = CTkComboBox(time_frame, values=[f"{i:02d}" for i in range(24)], variable=hour_var, width=60)
            hour_combo.pack(side="left", padx=5)
            
            CTkLabel(time_frame, text="分:").pack(side="left", padx=5)
            minute_combo = CTkComboBox(time_frame, values=[f"{i:02d}" for i in range(0, 60, 5)], variable=minute_var, width=60)
            minute_combo.pack(side="left", padx=5)
            
            # 星期选择（仅当频率为每周时显示）
            week_frame = CTkFrame(dialog)
            week_frame.pack(fill="x", padx=20, pady=5)
            
            days_var = []
            days_frame = CTkFrame(week_frame)
            days_frame.pack(fill="x", pady=5)
            
            day_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
            for i, day_name in enumerate(day_names):
                var = ctk.BooleanVar()
                cb = CTkCheckBox(days_frame, text=day_name, variable=var)
                cb.grid(row=i//4, column=i%4, padx=2, pady=2)
                days_var.append(var)
            
            # 默认选中周一
            days_var[0].set(True)
            
            def update_week_visibility():
                """根据频率显示/隐藏星期选择"""
                if frequency_var.get() == "WEEKLY":
                    week_frame.pack(fill="x", padx=20, pady=5)
                else:
                    week_frame.pack_forget()
            
            frequency_var.trace('w', lambda *args: update_week_visibility())
            update_week_visibility()
            
            # 按钮
            button_frame = CTkFrame(dialog)
            button_frame.pack(side="bottom", pady=20)
            
            def save_schedule():
                """保存定时配置并注册任务"""
                try:
                    # 获取配置
                    frequency = frequency_var.get()
                    hour = hour_var.get()
                    minute = minute_var.get()
                    time_str = f"{hour}:{minute}"
                    
                    # 更新任务配置
                    task["schedule_config"]["enabled"] = True
                    task["schedule_config"]["frequency"] = frequency
                    task["schedule_config"]["time"] = time_str
                    
                    if frequency == "WEEKLY":
                        selected_days_indices = [i for i, var in enumerate(days_var) if var.get()]
                        if not selected_days_indices:
                            CTkMessagebox(title="错误", message="请选择至少一个星期几", icon="warning")
                            return
                        
                        day_names = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
                        days_str = ",".join([day_names[i] for i in selected_days_indices])
                        
                        success = register_scheduled_task_advanced(task["name"], frequency, time_str, days_str)
                        if success:
                            add_task_config(task)
                            CTkMessagebox(title="成功", message=f"已注册任务 '{task['name']}' 的每周定时计划", icon="check")
                            dialog.destroy()
                            self.refresh_task_list()
                        else:
                            CTkMessagebox(title="失败", message="注册每周定时任务失败", icon="cancel")
                    else:  # DAILY
                        success = register_scheduled_task_advanced(task["name"], frequency, time_str)
                        if success:
                            add_task_config(task)
                            CTkMessagebox(title="成功", message=f"已注册任务 '{task['name']}' 的每日定时计划", icon="check")
                            dialog.destroy()
                            self.refresh_task_list()
                        else:
                            CTkMessagebox(title="失败", message="注册每日定时任务失败", icon="cancel")
                            
                except Exception as e:
                    CTkMessagebox(title="错误", message=f"注册定时任务时出错: {e}", icon="cancel")
            
            CTkButton(button_frame, text="取消", command=dialog.destroy, width=80).pack(side="left", padx=10)
            CTkButton(button_frame, text="确定", command=save_schedule, fg_color="green", width=80).pack(side="left", padx=10)
        
        def delete_task(self, task):
            """删除任务"""
            msg = CTkMessagebox(title="确认删除", message=f"确定要删除任务 '{task['name']}' 吗？", icon="question", option_1="否", option_2="是")
            if msg.get() == "是":
                try:
                    # 如果有定时任务，先删除Windows中的定时任务
                    if task["schedule_config"]["enabled"]:
                        delete_scheduled_task(task["name"])
                    
                    config = load_config()
                    config["tasks"] = [t for t in config["tasks"] if t["name"] != task["name"]]
                    save_config(config)
                    
                    CTkMessagebox(title="删除成功", message="任务已删除", icon="check")
                    self.refresh_task_list()
                except Exception as e:
                    CTkMessagebox(title="删除失败", message=f"删除任务失败: {e}", icon="cancel")


    def show_gui():
        """显示GUI界面"""
        app = TaskManagerApp()
        app.mainloop()
else:
    def show_gui():
        """GUI不可用时的提示"""
        print("GUI功能不可用，请安装CustomTkinter: pip install customtkinter")


# ==================== 首次运行配置向导 ====================
def show_first_time_setup():
    """首次运行时的配置向导"""
    if GUI_AVAILABLE:
        from tkinter import messagebox
        result = messagebox.askyesno(
            "首次运行配置",
            "检测到首次运行，是否现在配置示例任务？\n"
            "您也可以选择跳过，在主界面手动配置任务。"
        )
        if result:
            # 添加示例任务
            add_task_config(EXAMPLE_TASK_CONFIG)
            messagebox.showinfo("配置完成", "已添加示例任务，请在主界面查看和修改配置。")
        else:
            messagebox.showinfo("提示", "您可以在主界面手动添加任务配置。")
    else:
        print("首次运行提示：建议添加任务配置")
        print("示例任务配置已准备，您可以在代码中找到EXAMPLE_TASK_CONFIG作为参考")

# ==================== 主程序入口 ====================
def main():
    """主程序入口"""
    parser = argparse.ArgumentParser(description="百川数据助手")
    parser.add_argument("--headless", type=str, help="Headless模式，指定任务名")
    parser.add_argument("--test-task", type=str, help="测试指定任务")
    parser.add_argument("--list-tasks", action="store_true", help="列出所有任务")
    parser.add_argument("--register-task", type=str, help="注册定时任务")
    parser.add_argument("--unregister-task", type=str, help="注销定时任务")
    parser.add_argument("--first-time-setup", action="store_true", help="显示首次运行配置向导")
    
    args = parser.parse_args()

    # 如果是headless模式，重新配置日志，只输出到文件
    if args.headless:
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        logger.addHandler(logging.FileHandler(LOG_FILE, encoding='utf-8'))

    # 检查是否需要生成默认配置
    if not CONFIG_FILE.exists():
        logger.info("首次运行，生成默认配置")
        save_config(DEFAULT_CONFIG_TEMPLATE)
        
        # 如果没有命令行参数，显示首次运行配置向导
        if not any([args.headless, args.test_task, args.list_tasks, args.register_task, args.unregister_task]):
            if args.first_time_setup:
                show_first_time_setup()
    
    if args.headless:
        # Headless模式
        return run_headless(args.headless)
    elif args.test_task:
        # 测试任务
        success = execute_task(args.test_task)
        return 0 if success else 1
    elif args.list_tasks:
        # 列出任务
        config = load_config()
        tasks = config.get("tasks", [])
        print("当前配置的任务:")
        for task in tasks:
            print(f"  - {task['name']}")
        return 0
    elif args.register_task:
        # 注册定时任务
        return 0 if register_scheduled_task(args.register_task) else 1
    elif args.unregister_task:
        # 注销定时任务
        return 0 if unregister_scheduled_task(args.unregister_task) else 1
    elif args.first_time_setup:
        # 显示首次运行配置向导
        show_first_time_setup()
        return 0
    else:
        # GUI模式
        if GUI_AVAILABLE:
            show_gui()
        else:
            print(__doc__)
            print("\nGUI功能需要安装CustomTkinter:")
            print("pip install customtkinter")
            print("\n使用方法:")
            print("  --headless <任务名>     : Headless模式运行指定任务")
            # 移除了--test-task的说明
            print("  --list-tasks           : 列出所有任务")
            print("  --register-task <任务名> : 注册定时任务")
            print("  --unregister-task <任务名> : 注销定时任务")
            print("  --first-time-setup      : 显示首次运行配置向导")
        return 0

if __name__ == "__main__":
    sys.exit(main())