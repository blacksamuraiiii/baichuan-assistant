# -*- coding: utf-8 -*-
"""
工具函数模块 - 将app.py中的工具函数分离出来以减少主文件体积
"""

import os
import sys
import time
import subprocess
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

# ==================== 路径管理 ====================
def get_paths():
    """获取应用相关路径"""
    if getattr(sys, 'frozen', False):
        # 打包后：内部文件在_MEIPASS临时目录，外部文件在exe同级目录
        INTERNAL_DIR = Path(sys._MEIPASS) if hasattr(sys, '_MEIPASS') else Path(sys.executable).parent
        EXTERNAL_DIR = Path(sys.executable).parent
    else:
        # 直接运行时：所有文件都在脚本所在目录
        INTERNAL_DIR = Path(__file__).parent
        EXTERNAL_DIR = Path(__file__).parent

    return INTERNAL_DIR, EXTERNAL_DIR

# ==================== 加密工具 ====================
def ensure_secret_key():
    """确保加密密钥存在，不存在则生成"""
    _, EXTERNAL_DIR = get_paths()
    INTERNAL_DIR, _ = get_paths()
    SECRET_KEY_FILE = INTERNAL_DIR / "secret.key"

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

# ==================== 缓存系统 ====================
_current_cache = {}

def get_cached_data(api_name: str = "API1") -> Optional[pd.DataFrame]:
    """获取当前任务的缓存数据"""
    return _current_cache.get(api_name)

def set_cached_data(api_name: str, df: pd.DataFrame):
    """设置当前任务的缓存数据"""
    _current_cache[api_name] = df

def clear_cache():
    """清空缓存"""
    _current_cache.clear()

# ==================== 任务锁机制 ====================
def _manage_lock(task_name: str, acquire: bool = True) -> bool:
    """统一的锁管理函数"""
    _, EXTERNAL_DIR = get_paths()
    lock_file = EXTERNAL_DIR / "locks" / f"{task_name}.lock"

    if acquire:
        # 获取锁
        if lock_file.exists():
            try:
                # 检查是否过期（1小时）
                if datetime.now() - datetime.fromtimestamp(lock_file.stat().st_mtime) > timedelta(hours=1):
                    lock_file.unlink()
                else:
                    logger.info(f"任务 {task_name} 已被锁定")
                    return False
            except:
                return False

        try:
            lock_file.parent.mkdir(parents=True, exist_ok=True)
            if os.name == 'nt':  # Windows隐藏目录
                subprocess.run(['attrib', '+H', str(lock_file.parent)], shell=True, capture_output=True)

            lock_file.write_text(f"{os.getpid()}|{datetime.now()}", encoding='utf-8')
            logger.info(f"任务 {task_name} 锁定成功")
            return True
        except Exception as e:
            logger.error(f"锁定失败: {e}")
            return False
    else:
        # 释放锁
        try:
            if lock_file.exists():
                lock_file.unlink()
                logger.info(f"任务 {task_name} 锁释放")

            # 清理空目录
            if lock_file.parent.exists() and not any(lock_file.parent.iterdir()):
                lock_file.parent.rmdir()
            return True
        except Exception as e:
            logger.error(f"释放锁失败: {e}")
            return False

def acquire_lock(task_name: str) -> bool:
    """获取任务锁"""
    return _manage_lock(task_name, acquire=True)

def release_lock(task_name: str) -> bool:
    """释放任务锁"""
    return _manage_lock(task_name, acquire=False)

# ==================== 占位符处理 ====================
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

def _format_task_strings(texts: list, task_name: str) -> list:
    """批量替换任务相关字符串中的占位符"""
    today = date.today().strftime("%Y%m%d")
    replacements = {
        "{date}": today,
        "{taskName}": task_name
    }

    result = []
    for text in texts:
        formatted_text = text
        for placeholder, value in replacements.items():
            formatted_text = formatted_text.replace(placeholder, value)
        result.append(formatted_text)

    return result

# ==================== API数据获取 ====================
def fetch_api_data(task_config: Dict, api_name: str = "API1", use_cache: bool = True) -> Optional[pd.DataFrame]:
    """从指定API获取数据"""
    # 检查缓存
    if use_cache:
        cached_df = get_cached_data(api_name)
        if cached_df is not None:
            logger.info(f"使用缓存的DataFrame: {api_name}")
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

    # 直接使用headers，不再解密
    decrypted_headers = headers

    logger.info(f"正在从API获取数据: {url} ({api_name})")

    try:
        response = requests.post(
            url,
            headers=decrypted_headers,
            timeout=timeout,
            verify=verify_ssl
        )
        response.raise_for_status()

        response_data = response.json()
        if response_data.get('success') and 'value' in response_data:
            df = pd.DataFrame(response_data['value'])
            logger.info(f"API数据获取成功: {api_name}, 共 {len(df)} 行数据")

            # 数据校验
            required_fields = task_config["data_config"].get("required_fields", [])
            if required_fields:
                missing_fields = [field for field in required_fields if field not in df.columns]
                if missing_fields:
                    logger.error(f"数据缺少必要字段: {missing_fields}")
                    return None

            # 缓存DataFrame
            set_cached_data(api_name, df)
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

# ==================== Windows任务计划工具 ====================
def register_scheduled_task(task_name: str, frequency: str = "DAILY", time_str: str = "18:00", day_of_week: str = None) -> bool:
    """注册Windows定时任务（主入口函数）"""
    try:
        # 构建任务计划命令
        _, EXTERNAL_DIR = get_paths()
        if getattr(sys, 'frozen', False):
            exe_path = str(Path(sys.executable).resolve())
        else:
            exe_path = str(Path(__file__).parent / "app.py")

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
            logger.info(f"定时任务注册成功: {task_name} ({frequency} {time_str})")
            return True
        else:
            logger.error(f"定时任务注册失败: {stderr_text}")
            return False

    except Exception as e:
        logger.error(f"注册定时任务时出错: {e}")
        return False

def get_task_status(task_name: str) -> str:
    """获取任务在Windows任务计划程序中的状态"""
    task_name_escaped = f"KW_{task_name.replace(' ', '_')}"

    try:
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

def enable_scheduled_task(task_name: str) -> bool:
    """启用已禁用的Windows定时任务"""
    task_name_escaped = f"KW_{task_name.replace(' ', '_')}"

    try:
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
    task_name_escaped = f"KW_{task_name.replace(' ', '_')}"

    try:
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
    task_name_escaped = f"KW_{task_name.replace(' ', '_')}"

    try:
        # 直接删除任务，使用 /F 强制删除
        delete_cmd = ['schtasks', '/delete', '/tn', task_name_escaped, '/f']
        logger.info(f"执行命令: {' '.join(delete_cmd)}")

        result = subprocess.run(delete_cmd, capture_output=True, shell=True)

        try:
            stderr_text = result.stderr.decode('utf-8', errors='ignore') if isinstance(result.stderr, bytes) else str(result.stderr or '')
        except:
            stderr_text = str(result.stderr or '')

        # 如果返回码为0，说明成功。如果返回码不为0，但错误信息包含"找不到"，也视为成功（任务本就不存在）
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

# ==================== 导入logger以供工具函数使用 ====================
# 在app.py中会设置logger
logger = None

def set_logger(logger_instance):
    """设置logger实例"""
    global logger
    logger = logger_instance