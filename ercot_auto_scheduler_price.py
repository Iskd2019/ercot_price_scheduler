import logging
import sys
import signal
import os
import subprocess
from threading import Event
from apscheduler.schedulers.background import BackgroundScheduler

# 设置时区为美国中部时间（ERCOT所在）
os.environ['TZ'] = 'America/Chicago'

# 配置日志输出到控制台
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# 停止事件（用于优雅退出）
stop_event = Event()

# 所有脚本文件（依次执行）
scripts = [
    "update_rtd_forecast_live.py",
    "update_daily_dam_price.py",
    "update_daily_15_mins_energy_price.py",
    "update_latest_lmp_5min.py"

]
# 脚本执行函数（增加超时控制）
def run_all_scripts():
    logging.info("\n===== 正在执行所有数据处理脚本 =====")
    for script in scripts:
        try:
            logging.info(f"▶️ 正在运行: {script}")
            # 设置超时时间，例如 180 秒（3 分钟）
            subprocess.run([sys.executable, script], check=True, timeout=180)
        except subprocess.TimeoutExpired as e:
            logging.error(f"⏰ 脚本执行超时: {script}，已强制终止，错误: {e}")
        except subprocess.CalledProcessError as e:
            logging.error(f"❌ 脚本执行失败: {script}，错误: {e}")
        except Exception as e:
            logging.error(f"⚠️ 未知错误: {script}，错误: {e}")
    logging.info("✅ 所有脚本执行完成\n")
'''
# 脚本执行函数
def run_all_scripts():
    logging.info("\n===== 正在执行所有数据处理脚本 =====")
    for script in scripts:
        try:
            logging.info(f"▶️ 正在运行: {script}")
            subprocess.run([sys.executable, script], check=True)
        except subprocess.TimeoutExpired as e:
            logging.error(f"⏰ 脚本执行超时: {script}，强制终止，错误: {e}")
        except subprocess.CalledProcessError as e:
            logging.error(f"❌ 脚本失败: {script}，错误: {e}")
    logging.info("✅ 所有脚本执行完成\n")
'''

# APScheduler 启动逻辑
def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_all_scripts, 'interval', minutes=1, max_instances=1)
    scheduler.start()

    logging.info("🟢 定时任务已启动，每5分钟运行一次所有脚本")

    # 捕捉退出信号
    def handle_exit(signum, frame):
        logging.info("🛑 接收到退出信号，准备关闭...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    stop_event.wait()
    scheduler.shutdown()
    logging.info("✅ 调度器已关闭")

if __name__ == "__main__":
    start_scheduler()
