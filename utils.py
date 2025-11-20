# -*- coding: utf-8 -*-
"""
工具函数模块 - 将app.py中的工具函数分离出来以减少主文件体积
"""

# -*- coding: utf-8 -*-
"""
工具函数模块 - 将app.py中的工具函数分离出来以减少主文件体积
"""

import os
import sys
import time
import json
import subprocess
import pandas as pd
import requests
import ijson
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

class ChainedStream:
    """辅助类：用于连接预读取的chunk和原始流"""
    def __init__(self, first_chunk, stream):
        self.buffer = first_chunk
        self.stream = stream
        
    def read(self, size=-1):
        if self.buffer:
            if size == -1 or size >= len(self.buffer):
                result = self.buffer
                self.buffer = b""
                if size != -1:
                    # 需要读取更多
                    remaining = size - len(result)
                    if remaining > 0:
                        chunk = self.stream.read(remaining)
                        if chunk:
                            result += chunk
                else:
                    chunk = self.stream.read()
                    if chunk:
                        result += chunk
                return result
            else:
                result = self.buffer[:size]
                self.buffer = self.buffer[size:]
                return result
        return self.stream.read(size)

# ==================== API数据获取 ====================
def fetch_api_data(task_config: Dict, api_name: str = "API1", use_cache: bool = True) -> Optional[pd.DataFrame]:
    """从指定API获取数据 - 优化版：流式处理大数据，防止内存溢出（适用于不分页API）"""
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
    timeout = api_config.get("timeout", 120)  # 增加超时时间，处理大数据
    verify_ssl = api_config.get("verify_ssl", True)
    max_records = api_config.get("max_records", 100000)  # 最大记录数限制

    # 直接使用headers，不再解密
    decrypted_headers = headers

    logger.info(f"正在从API获取数据: {url} ({api_name})")

    try:
        logger.info(f"开始请求API数据，最大记录数限制: {max_records}")
        
        # 使用流式请求
        response = requests.post(
            url,
            headers=decrypted_headers,
            timeout=timeout,
            verify=verify_ssl,
            stream=True  # 开启流式模式
        )
        response.raise_for_status()

        # 预读取一小部分数据以检测结构和错误
        # requests的raw是urllib3的HTTPResponse，通常支持read
        first_chunk = response.raw.read(2048)
        
        # 检查是否是错误响应（通常错误响应很短且包含 success: false）
        try:
            preview = first_chunk.decode('utf-8', errors='ignore')
            clean_preview = ''.join(preview.split())
            
            # 如果看起来像错误响应
            if '"success":false' in clean_preview or '"success":0' in clean_preview:
                # 读取剩余部分以便完整解析
                remaining = response.raw.read()
                full_content = first_chunk + remaining
                try:
                    error_data = json.loads(full_content)
                    logger.error(f"API返回错误: {error_data.get('message', '未知错误')}")
                except:
                    logger.error(f"API返回错误且无法解析: {preview[:200]}...")
                return None
                
        except Exception as e:
            logger.warning(f"预检查响应失败，继续尝试解析: {e}")

        # 构建链式流
        stream = ChainedStream(first_chunk, response.raw)
        
        # 确定解析路径
        # 默认假设是 value: [...]
        prefix = 'value.item'
        if '"value":{' in clean_preview or '"value":{"records":[' in clean_preview:
            prefix = 'value.records.item'
            
        logger.info(f"使用流式解析，路径: {prefix}")
        
        # 创建生成器
        records_iter = ijson.items(stream, prefix)
        
        # 使用流式处理函数
        return _process_stream_dataset(records_iter, task_config, api_name, max_records)
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API请求失败: {api_name} - {e}")
        return None
    except Exception as e:
        logger.error(f"数据处理失败: {api_name} - {e}")
        return None

def _process_stream_dataset(records_iter, task_config: Dict, api_name: str, max_records: int) -> Optional[pd.DataFrame]:
    """处理流式数据集 - 分批构建DataFrame"""
    logger.info(f"开始流式处理数据: {api_name}")
    
    try:
        # 分批构建DataFrame以减少内存峰值
        batch_size = 10000  # 每批10000条
        dataframes = []
        current_batch = []
        total_count = 0
        
        for record in records_iter:
            current_batch.append(record)
            total_count += 1
            
            # 达到批次大小时处理
            if len(current_batch) >= batch_size:
                batch_df = pd.DataFrame(current_batch)
                dataframes.append(batch_df)
                current_batch = [] # 清空当前批次
                
                logger.info(f"已处理数据: {total_count} 条")
                
                # 立即清理内存
                import gc
                gc.collect()
                
            # 检查最大记录数限制
            if total_count >= max_records:
                logger.warning(f"达到最大记录数限制 ({max_records})，停止读取")
                break
        
        # 处理剩余数据
        if current_batch:
            batch_df = pd.DataFrame(current_batch)
            dataframes.append(batch_df)
            logger.info(f"处理剩余数据，总计: {total_count} 条")
            
        if not dataframes:
            logger.warning(f"未获取到任何数据: {api_name}")
            return None
            
        # 合并所有批次
        logger.info("开始合并数据批次...")
        final_df = pd.concat(dataframes, ignore_index=True)
        
        # 清理临时DataFrame
        del dataframes
        del current_batch
        import gc
        gc.collect()
        
        return _finalize_dataframe(final_df, task_config, api_name)
        
    except Exception as e:
        logger.error(f"流式数据集处理失败: {api_name} - {e}")
        return None

def _process_small_dataset(records: List, task_config: Dict, api_name: str) -> Optional[pd.DataFrame]:
    """处理小数据集 - 直接构建"""
    logger.info(f"小数据集直接处理: {len(records)} 条记录")
    
    try:
        df = pd.DataFrame(records)
        return _finalize_dataframe(df, task_config, api_name)
        
    except Exception as e:
        logger.error(f"小数据集处理失败: {api_name} - {e}")
        return None

def _finalize_dataframe(df: pd.DataFrame, task_config: Dict, api_name: str) -> Optional[pd.DataFrame]:
    """最终DataFrame处理和验证"""
    try:
        # 数据去重（防止API返回重复数据）
        if not df.empty:
            initial_count = len(df)
            df = df.drop_duplicates()
            if len(df) < initial_count:
                logger.info(f"数据去重: {initial_count} -> {len(df)} 条记录")
        
        logger.info(f"数据处理完成: {api_name}, 共 {len(df)} 行数据")
        
        # 数据校验
        required_fields = task_config["data_config"].get("required_fields", [])
        if required_fields:
            missing_fields = [field for field in required_fields if field not in df.columns]
            if missing_fields:
                logger.error(f"数据缺少必要字段: {missing_fields}")
                return None

        # 内存使用情况报告
        memory_usage = df.memory_usage(deep=True).sum() / 1024 / 1024  # MB
        logger.info(f"DataFrame内存使用: {memory_usage:.2f} MB")
        
        # 缓存结果
        set_cached_data(api_name, df)
        
        # 最终内存清理
        import gc
        gc.collect()
        
        return df
        
    except Exception as e:
        logger.error(f"最终DataFrame处理失败: {api_name} - {e}")
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

# ==================== 邮件发送工具 ====================
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from io import BytesIO

def send_email(task_config: Dict, data_frames: Dict[str, pd.DataFrame] = None, attachment_path: str = None) -> bool:
    """统一邮件发送函数 - 支持DataFrame直接发送或文件附件"""

    email_config = task_config["email_config"]
    sender_config = email_config["sender"]
    recipients = email_config["recipients"]

    # 验证配置
    stored_password = sender_config.get("password", "")
    if not stored_password:
        logger.error("发件人密码为空")
        return False

    # 解密密码
    try:
        if stored_password.startswith("gAAAAA"):
            # 加密过的密码
            password = decrypt_data(stored_password)
        else:
            # 明文密码（向后兼容）
            password = stored_password
    except Exception as e:
        logger.error(f"密码解密失败: {e}")
        return False

    to_list = recipients.get("to", [])
    if not to_list:
        logger.error("收件人列表为空")
        return False

    # 批量处理占位符
    task_name = task_config["name"]
    if data_frames:
        # 使用DataFrame的情况
        body = replace_sheet_variables(task_config, data_frames)
        # 处理邮件正文中的 {taskName} 变量
        body = replace_placeholders(body, task_name)
        subject, attachment_name = _format_task_strings(
            [email_config["subject"], email_config["attachment_name"]],
            task_name
        )
        return _send_email_internal(task_config, subject, body,
                                   _create_excel_attachment(task_config, data_frames),
                                   attachment_name, sender_config, to_list,
                                   recipients.get("cc", []), recipients.get("bcc", []), password)

    elif attachment_path:
        # 使用文件附件的情况
        body, subject, attachment_name = _format_task_strings(
            [email_config["body"], email_config["subject"], email_config["attachment_name"]],
            task_name
        )
        # 再次处理邮件正文中的 {Sheet1} 等变量（如果有的话）
        # 这里不进行Sheet变量替换，因为只有DataFrame才处理Sheet变量
        return _send_email_with_file(task_config, subject, body, attachment_path,
                                   attachment_name, sender_config, to_list,
                                   recipients.get("cc", []), recipients.get("bcc", []), password)

    else:
        logger.error("邮件发送失败：未提供数据或附件")
        return False

def _create_excel_attachment(task_config: Dict, data_frames: Dict[str, pd.DataFrame]) -> bytes:
    """创建Excel附件数据"""
    buffer = BytesIO()

    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        sheet_names = task_config["data_config"].get("sheet_names", [])

        for i, (api_name, df) in enumerate(data_frames.items()):
            if df is not None and not df.empty:
                sheet_name = sheet_names[i] if i < len(sheet_names) else f"Sheet{i+1}"
                # 清理sheet名称
                sheet_name = sheet_name[:31].replace('/', '-').replace('\\', '-').replace('?', '').replace('*', '-').replace('[', '(').replace(']', ')')
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                logger.info(f"Sheet '{sheet_name}' 写入内存: {len(df)} 行数据")

    return buffer.getvalue()

def replace_sheet_variables(task_config: Dict, data_frames: Dict[str, pd.DataFrame]) -> str:
    """替换邮件正文中的表格变量 - 支持多种变量名指向同一个数据框

    支持的变量名格式（都指向同一个数据框）:
    - 配置的sheet名称: {111}
    - 默认sheet名称: {Sheet1}
    - API名称: {API1}
    - 表格索引名称: {Table1}
    """
    body = task_config["email_config"]["body"]
    sheet_names = task_config["data_config"].get("sheet_names", [])

    # 为每个数据框生成HTML表格，并支持多种变量名指向它
    for i, (api_name, df) in enumerate(data_frames.items()):
        if df is not None and not df.empty:
            # 获取配置的sheet名称和默认名称
            configured_sheet_name = sheet_names[i] if i < len(sheet_names) else f"Sheet{i+1}"
            default_sheet_name = f"Sheet{i+1}"

            # 清理配置的sheet名称中的非法字符
            clean_configured_name = configured_sheet_name[:31].replace('/', '-').replace('\\', '-').replace('?', '').replace('*', '-').replace('[', '(').replace(']', ')')

            # 生成HTML表格 - 所有变量名都指向同一个数据框
            html_table = df.head(10).to_html(index=False, escape=True)

            # 支持多种变量名指向同一个数据框：
            # 这样用户可以使用任何一种变量名，都会被替换为同一个HTML表格
            variable_names = [
                f"{{{clean_configured_name}}}",  # 配置的sheet名称，如{111}
                f"{{{default_sheet_name}}}",     # 默认sheet名称，如{Sheet1}
                f"{{{api_name}}}",               # API名称，如{API1}
                f"{{Table{i+1}}}"                # 表格索引名称，如{Table1}
            ]

            # 所有这些变量名都替换为同一个HTML表格
            for var_name in variable_names:
                body = body.replace(var_name, html_table)

    return body

def _send_email_internal(task_config: Dict, subject: str, body: str, attachment_data: bytes,
                        attachment_name: str, sender_config: Dict, to_list: List, cc_list: List, bcc_list: List, password: str) -> bool:
    """内部邮件发送函数 - 使用内存数据"""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.application import MIMEApplication

    smtp_server = task_config.get("smtp_server", "smtp.chinatelecom.cn")
    smtp_port = task_config.get("smtp_port", 465)

    try:
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = sender_config["email"]
        msg['To'] = ','.join(to_list)
        if cc_list:
            msg['Cc'] = ','.join(cc_list)

        # 添加正文
        body_mime = MIMEText(body, 'html', 'utf-8')
        msg.attach(body_mime)

        # 添加附件
        if attachment_data:
            attachment = MIMEApplication(attachment_data, _subtype='xlsx')
            attachment.add_header('Content-Disposition', 'attachment', filename=attachment_name)
            msg.attach(attachment)
            logger.info(f"使用内存数据作为附件: {len(attachment_data)} bytes")

        # 发送邮件
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as smtp:
            smtp.login(sender_config["email"], password)
            all_recipients = to_list + cc_list + bcc_list
            smtp.sendmail(sender_config["email"], all_recipients, msg.as_string())

        logger.info(f"邮件发送成功，收件人: {len(to_list)}人, 抄送: {len(cc_list)}人")
        return True

    except Exception as e:
        logger.error(f"邮件发送失败: {e}")
        return False

def _send_email_with_file(task_config: Dict, subject: str, body: str, attachment_path: str,
                         attachment_name: str, sender_config: Dict, to_list: List, cc_list: List, bcc_list: List, password: str) -> bool:
    """内部邮件发送函数 - 使用文件附件"""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.application import MIMEApplication

    smtp_server = task_config.get("smtp_server", "smtp.chinatelecom.cn")
    smtp_port = task_config.get("smtp_port", 465)

    try:
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = sender_config["email"]
        msg['To'] = ','.join(to_list)
        if cc_list:
            msg['Cc'] = ','.join(cc_list)

        # 添加正文
        body_mime = MIMEText(body, 'html', 'utf-8')
        msg.attach(body_mime)

        # 添加文件附件
        if Path(attachment_path).exists():
            with open(attachment_path, 'rb') as f:
                attachment = MIMEApplication(f.read(), _subtype='xlsx')
                attachment.add_header('Content-Disposition', 'attachment', filename=attachment_name)
                msg.attach(attachment)
            logger.info(f"使用文件作为附件: {attachment_path}")

        # 发送邮件
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as smtp:
            smtp.login(sender_config["email"], password)
            all_recipients = to_list + cc_list + bcc_list
            smtp.sendmail(sender_config["email"], all_recipients, msg.as_string())

        logger.info(f"邮件发送成功，收件人: {len(to_list)}人, 抄送: {len(cc_list)}人")
        return True

    except Exception as e:
        logger.error(f"邮件发送失败: {e}")
        return False

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

# ==================== 配置常量 ====================
DEFAULT_CONFIG_TEMPLATE = {
    "version": "1.0.0",
    "tasks": [],
    "settings": {
        "default_smtp_server": "smtp.chinatelecom.cn",
        "default_smtp_port": 465,
        "default_timeout": 120,  # 增加到120秒，适配大数据API调用
        "retry_attempts": 3,
        "retry_delay": 5
    }
}

TASK_TEMPLATE = {
    "name": "",
    "api_configs": [
        {
            "name": "API1",
            "url": "",
            "headers": {"appKey": "", "appSecret": ""},
            "timeout": 120,  # 增加默认超时时间，适配大数据
            "verify_ssl": True,
            "max_records": 100000,  # 新增：单次获取最大记录数限制
            "streaming_threshold": 50000  # 新增：流式处理阈值
        }
    ],
    "data_config": {
        "filename_pattern": "{taskName}_{date}.xlsx",
        "sheet_names": ["Sheet1"],
        "required_fields": []  # 新增：数据校验字段
    },
    "email_config": {
        "sender": {"email": "", "password": ""},
        "recipients": {"to": [], "cc": [], "bcc": []},
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

# ==================== 配置管理工具 ====================
def load_config() -> Dict:
    """加载配置文件"""
    _, EXTERNAL_DIR = get_paths()
    CONFIG_FILE = EXTERNAL_DIR / "config.json"

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
    _, EXTERNAL_DIR = get_paths()
    CONFIG_FILE = EXTERNAL_DIR / "config.json"

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

# ==================== 任务执行工具 ====================
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
        # 1. 获取API数据（带重试）
        data_frames = None
        for attempt in range(3):
            try:
                data_frames = fetch_all_api_data(task_config, use_cache=True)
                if data_frames and not any(df is None for df in data_frames.values()):
                    break
            except Exception as e:
                logger.warning(f"第 {attempt + 1} 次数据获取失败: {e}")
                if attempt < 2:
                    logger.info("等待5秒后重试...")
                    time.sleep(5)

        if not data_frames or any(df is None for df in data_frames.values()):
            logger.error(f"任务 {task_name} 数据获取失败")
            return False

        # 2. 发送邮件（带重试）
        email_success = False
        for attempt in range(3):
            try:
                if send_email(task_config, data_frames=data_frames):
                    email_success = True
                    break
            except Exception as e:
                logger.warning(f"第 {attempt + 1} 次邮件发送失败: {e}")
                if attempt < 2:
                    logger.info("等待5秒后重试...")
                    time.sleep(5)

        if email_success:
            logger.info(f"任务 {task_name} 执行成功")
            return True
        else:
            logger.error(f"任务 {task_name} 邮件发送失败")
            return False

    except Exception as e:
        logger.error(f"任务 {task_name} 执行异常: {e}")
        return False
    finally:
        release_lock(task_name)
        clear_cache()

# ==================== 其他工具函数 ====================
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

# ==================== 导入logger以供工具函数使用 ====================
# 在app.py中会设置logger
logger = None

def set_logger(logger_instance):
    """设置logger实例"""
    global logger
    logger = logger_instance