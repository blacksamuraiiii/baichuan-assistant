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
import logging
import argparse
import time
import pandas as pd
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Any

# 导入工具函数
from utils import (
    get_paths, ensure_secret_key, encrypt_data, decrypt_data,
    get_cached_data, set_cached_data, clear_cache,
    acquire_lock, release_lock, replace_placeholders, _format_task_strings,
    fetch_api_data, fetch_all_api_data, generate_excel_file_with_sheets,
    register_scheduled_task, get_task_status, enable_scheduled_task,
    disable_scheduled_task, delete_scheduled_task, get_scheduled_tasks,
    set_logger, send_email, generate_excel_file, load_config, save_config,
    get_task_config, add_task_config, execute_task, unregister_scheduled_task,
    run_headless, DEFAULT_CONFIG_TEMPLATE, TASK_TEMPLATE
)

# 导入GUI相关（可选，如果安装了CustomTkinter）
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
INTERNAL_DIR, EXTERNAL_DIR = get_paths()
CONFIG_FILE = EXTERNAL_DIR / "config.json"
SECRET_KEY_FILE = INTERNAL_DIR / "secret.key"
LOG_FILE = EXTERNAL_DIR / "app.log"

# ==================== 内置默认配置 ====================
# 配置常量已移至 utils.py

# ==================== 日志配置 ====================
def setup_logging():
    """配置日志系统"""
    try:
        # 确保日志目录存在
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)s | %(message)s',
            handlers=[
                logging.FileHandler(LOG_FILE, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        return logging.getLogger(__name__)
    except Exception as e:
        # 如果文件日志失败，只使用控制台日志
        print(f"警告: 文件日志配置失败: {e}")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)s | %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        return logging.getLogger(__name__)

logger = setup_logging()
# 设置utils模块的logger
set_logger(logger)

# ==================== 配置管理 ====================
# 配置管理相关函数已移至 utils.py

# ==================== 核心执行流程 ====================
# 核心执行流程相关函数已移至 utils.py

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
            self.geometry("800x650")  # 增加高度确保底部按钮显示完整
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
                "headers": {"appKey": "", "appSecret": ""},
                "timeout": 30,
                "verify_ssl": True
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

        def toggle_password_visibility(self):
            """切换密码显示/隐藏"""
            self.password_visible = not self.password_visible
            current_content = self.password_entry.get()

            if self.password_visible:
                # 显示密码
                if current_content.startswith("●") and self.stored_password:
                    # 当前显示的是星号，需要解密并显示真实密码
                    try:
                        if self.stored_password.startswith("gAAAAA"):
                            # 解密真实密码
                            real_password = decrypt_data(self.stored_password)
                            self.password_entry.delete(0, "end")
                            self.password_entry.insert(0, real_password)
                        else:
                            # 明文密码
                            self.password_entry.delete(0, "end")
                            self.password_entry.insert(0, self.stored_password)
                    except Exception:
                        # 解密失败，保持原样
                        pass
                self.password_entry.configure(show="")
                self.eye_button.configure(text="👁")
            else:
                # 隐藏密码
                if not current_content.startswith("●"):
                    # 当前显示的是真实密码，需要转换为星号显示
                    if current_content:
                        # 用户修改了密码，需要更新存储的密码
                        self.password_has_value = True
                        encrypted_password = encrypt_data(current_content)
                        self.stored_password = encrypted_password
                        # 显示星号
                        self.password_entry.delete(0, "end")
                        self.password_entry.insert(0, "●" * min(len(current_content), 8))
                    else:
                        # 密码为空
                        self.password_has_value = False
                        self.stored_password = ""

                self.password_entry.configure(show="*")
                self.eye_button.configure(text="*")

        def on_password_focus_in(self, event):
            """密码输入框获得焦点时的处理"""
            current_content = self.password_entry.get()
            # 如果当前显示的是星号（表示已设置的密码），则清空让用户重新输入
            if current_content and current_content.startswith("●"):
                self.password_entry.delete(0, "end")

        def on_password_key_press(self, event):
            """密码输入框按键时的处理"""
            # 如果用户开始输入且之前显示的是星号，说明用户在修改密码
            current_content = self.password_entry.get()
            if current_content and not current_content.startswith("●"):
                self.password_has_value = True

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

            # 密码输入框容器
            password_container = CTkFrame(sender_frame)
            password_container.grid(row=1, column=1, padx=5, pady=2, sticky="ew")
            password_container.grid_columnconfigure(0, weight=1)

            self.password_entry = CTkEntry(password_container, width=250, show="*")
            self.password_entry.grid(row=0, column=0, sticky="ew", padx=2)

            # 绑定焦点事件，当用户开始输入时清除显示的星号
            self.password_entry.bind("<FocusIn>", self.on_password_focus_in)
            self.password_entry.bind("<Key>", self.on_password_key_press)

            # 眼睛图标按钮
            self.password_visible = False
            self.password_has_value = False  # 标记密码是否已设置
            self.stored_password = ""  # 存储真实的加密密码
            self.eye_button = CTkButton(password_container, text="*", width=45,
                                      command=self.toggle_password_visibility)
            self.eye_button.grid(row=0, column=1, padx=2)

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
            CTkLabel(body_header_frame, text="提示：支持的变量 - {Sheet1}(或重命名后的表名)，都会替换为对应数据表格",
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
                self.load_current_step()  # 加载新步骤的数据

        def next_step(self):
            """下一步"""
            if self.current_step < len(self.steps) - 1:
                self.save_current_step()
                self.show_step(self.current_step + 1)
                self.load_current_step()  # 加载新步骤的数据

        def go_to_step(self, step_index):
            """跳转到指定步骤"""
            if step_index != self.current_step:
                self.save_current_step()
                self.show_step(step_index)
                self.load_current_step()  # 加载新步骤的数据

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
                # 检查是否是显示的星号（表示密码已设置但用户没有修改）
                if password and password.startswith("●") and self.password_has_value:
                    # 用户没有修改密码，保持原有的加密密码不变
                    self.task_config["email_config"]["sender"]["password"] = self.stored_password
                else:
                    # 用户输入了新密码或清空了密码
                    self.password_has_value = bool(password)
                    if password:
                        encrypted_password = encrypt_data(password)
                        self.task_config["email_config"]["sender"]["password"] = encrypted_password
                        self.stored_password = encrypted_password  # 更新存储的密码
                    else:
                        self.task_config["email_config"]["sender"]["password"] = ""
                        self.stored_password = ""  # 清空存储的密码

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
                        
                        self.password_entry.delete(0, "end")
                        self.password_has_value = False
                        self.stored_password = ""
                        if "password" in sender:
                            stored_password = sender.get("password", "")
                            if stored_password:
                                try:
                                    # 尝试解密密码（支持向后兼容）
                                    if stored_password.startswith("gAAAAA"):
                                        # 加密过的密码
                                        decrypted_password = decrypt_data(stored_password)
                                        # 显示为星号，表示已设置密码
                                        self.password_entry.insert(0, "●" * min(len(decrypted_password), 8))
                                        self.password_has_value = True
                                        self.stored_password = stored_password  # 保存加密的密码
                                    else:
                                        # 明文密码（向后兼容），直接显示星号
                                        self.password_entry.insert(0, "●" * min(len(stored_password), 8))
                                        self.password_has_value = True
                                        # 同时升级为加密存储
                                        encrypted_password = encrypt_data(stored_password)
                                        sender["password"] = encrypted_password
                                        self.stored_password = encrypted_password  # 保存升级后的加密密码
                                except Exception:
                                    # 解密失败，可能是明文密码，显示为星号
                                    self.password_entry.insert(0, "●" * 6)
                                    self.password_has_value = True
                                    self.stored_password = stored_password  # 保存原密码
                            else:
                                # 空密码
                                self.password_entry.insert(0, "")

                        # 确保密码始终隐藏显示
                        self.password_entry.configure(show="*")
                        self.password_visible = False
                        if hasattr(self, 'eye_button'):
                            self.eye_button.configure(text="*")
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

        def preview_data(self):
            """预览数据"""
            self.save_current_step()  # 保存包括sheet名称的配置
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
                # 保存当前步骤的配置
                self.save_current_step()

                # 获取所有API数据
                data_frames = fetch_all_api_data(self.task_config, use_cache=True)
                if data_frames and any(df is not None for df in data_frames.values()):
                    from tkinter import filedialog
                    from pathlib import Path

                    # 生成默认文件名（使用用户配置的文件名模式）
                    filename_pattern = self.filename_entry.get().strip() if hasattr(self, 'filename_entry') else "{taskName}_{date}.xlsx"
                    default_filename = replace_placeholders(filename_pattern, self.task_config["name"])

                    file_path = filedialog.asksaveasfilename(
                        defaultextension=".xlsx",
                        initialfile=default_filename,
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
                # 测试运行前先获取所有API数据并缓存
                data_frames = fetch_all_api_data(self.task_config, use_cache=True)
                if not data_frames or all(df is None for df in data_frames.values()):
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

                        success = register_scheduled_task(task["name"], frequency, time_str, days_str)
                        if success:
                            add_task_config(task)
                            CTkMessagebox(title="成功", message=f"已注册任务 '{task['name']}' 的每周定时计划", icon="check")
                            dialog.destroy()
                            self.refresh_task_list()
                        else:
                            CTkMessagebox(title="失败", message="注册每周定时任务失败", icon="cancel")
                    else:  # DAILY
                        success = register_scheduled_task(task["name"], frequency, time_str)
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
            "检测到首次运行，是否现在配置任务？\n"
            "您也可以选择跳过，在主界面手动配置任务。"
        )
        if result:
            messagebox.showinfo("提示", "请在主界面点击'新建任务'开始配置。")
        else:
            messagebox.showinfo("提示", "您可以在主界面手动添加任务配置。")
    else:
        print("首次运行提示：建议添加任务配置")
        print("使用 --headless 参数运行任务或 --list-tasks 查看任务列表")

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

    # 确保日志目录存在
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"警告: 无法创建日志目录: {e}")

    # 如果是headless模式，重新配置日志，只输出到文件
    if args.headless:
        try:
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
            logger.addHandler(logging.FileHandler(LOG_FILE, encoding='utf-8'))
        except Exception as e:
            print(f"警告: Headless模式日志重配置失败: {e}")

    # 检查是否需要生成默认配置
    if not CONFIG_FILE.exists():
        try:
            logger.info("首次运行，生成默认配置")
            save_config(DEFAULT_CONFIG_TEMPLATE)
            logger.info("默认配置生成成功")
        except Exception as e:
            logger.error(f"生成默认配置失败: {e}")
            print(f"错误: 无法生成默认配置文件: {e}")
            return 1

        # 如果没有命令行参数，显示首次运行配置向导
        if not any([args.headless, args.test_task, args.list_tasks, args.register_task, args.unregister_task]):
            if args.first_time_setup:
                try:
                    show_first_time_setup()
                except Exception as e:
                    logger.error(f"显示首次运行配置向导失败: {e}")

    if args.headless:
        # Headless模式
        try:
            logger.info(f"Headless模式启动，执行任务: {args.headless}")
            result = run_headless(args.headless)
            logger.info(f"Headless任务 {args.headless} 完成，返回码: {result}")
            return result
        except Exception as e:
            logger.error(f"Headless任务执行失败: {e}")
            print(f"错误: Headless任务执行失败: {e}")
            return 1
    elif args.test_task:
        # 测试任务
        try:
            logger.info(f"测试任务: {args.test_task}")
            success = execute_task(args.test_task)
            if success:
                logger.info(f"任务 '{args.test_task}' 测试成功")
                return 0
            else:
                logger.error(f"任务 '{args.test_task}' 测试失败")
                return 1
        except Exception as e:
            logger.error(f"任务测试失败: {args.test_task} - {e}")
            print(f"错误: 任务测试失败: {e}")
            return 1
    elif args.list_tasks:
        # 列出任务
        try:
            config = load_config()
            tasks = config.get("tasks", [])
            print("当前配置的任务:")
            for task in tasks:
                print(f"  - {task['name']}")
            logger.info(f"列出任务成功，共 {len(tasks)} 个任务")
            return 0
        except Exception as e:
            logger.error(f"列出任务失败: {e}")
            print(f"错误: 无法列出任务: {e}")
            return 1
    elif args.register_task:
        # 注册定时任务
        try:
            logger.info(f"注册定时任务: {args.register_task}")
            success = register_scheduled_task(args.register_task)
            if success:
                logger.info(f"定时任务 '{args.register_task}' 注册成功")
                return 0
            else:
                logger.error(f"定时任务 '{args.register_task}' 注册失败")
                return 1
        except Exception as e:
            logger.error(f"注册定时任务失败: {args.register_task} - {e}")
            print(f"错误: 注册定时任务失败: {e}")
            return 1
    elif args.unregister_task:
        # 注销定时任务
        try:
            logger.info(f"注销定时任务: {args.unregister_task}")
            success = unregister_scheduled_task(args.unregister_task)
            if success:
                logger.info(f"定时任务 '{args.unregister_task}' 注销成功")
                return 0
            else:
                logger.error(f"定时任务 '{args.unregister_task}' 注销失败")
                return 1
        except Exception as e:
            logger.error(f"注销定时任务失败: {args.unregister_task} - {e}")
            print(f"错误: 注销定时任务失败: {e}")
            return 1
    elif args.first_time_setup:
        # 显示首次运行配置向导
        try:
            logger.info("显示首次运行配置向导")
            show_first_time_setup()
            logger.info("首次运行配置向导显示完成")
            return 0
        except Exception as e:
            logger.error(f"显示首次运行配置向导失败: {e}")
            print(f"错误: 显示首次运行配置向导失败: {e}")
            return 1
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
            print("  --list-tasks           : 列出所有任务")
            print("  --register-task <任务名> : 注册定时任务")
            print("  --unregister-task <任务名> : 注销定时任务")
            print("  --first-time-setup      : 显示首次运行配置向导")
        return 0

def global_exception_handler(exc_type, exc_value, exc_traceback):
    """全局异常处理器"""
    if issubclass(exc_type, KeyboardInterrupt):
        # 允许键盘中断（Ctrl+C）
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    
    # 记录异常信息
    logger.error("未捕获的异常", exc_info=(exc_type, exc_value, exc_traceback))
    print(f"\n程序发生未预期的错误: {exc_value}")
    print("请查看日志文件获取详细信息: app.log")
    print("您可以尝试以下操作:")
    print("1. 检查配置文件是否正确")
    print("2. 确保网络连接正常")
    print("3. 验证API凭据是否有效")
    print("4. 联系技术支持")

if __name__ == "__main__":
    # 设置全局异常处理器
    sys.excepthook = global_exception_handler
    
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n程序被用户中断")
        logger.info("程序被用户中断")
        sys.exit(0)
    except Exception as e:
        logger.error(f"程序主函数执行失败: {e}", exc_info=True)
        print(f"\n程序执行失败: {e}")
        print("请查看日志文件获取详细信息")
        sys.exit(1)