import os
import sys
import time
import socket
import subprocess

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON_EXE = sys.executable

processes = []

def is_port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0

def kill_process_on_port(port: int):
    """Освобождает порт на Windows, если остался процесс от прошлого запуска"""
    if sys.platform == "win32":
        try:
            out = subprocess.check_output(f'netstat -ano | findstr :{port}', shell=True).decode(errors="ignore")
            for line in out.strip().splitlines():
                parts = line.strip().split()
                if len(parts) >= 5 and "LISTENING" in line:
                    pid = parts[-1]
                    if pid != "0" and pid != str(os.getpid()):
                        print(f"  [Очистка] Освобождаю порт {port} (завершаю PID {pid})...")
                        subprocess.call(f"taskkill /F /PID {pid}", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass

def check_dependencies():
    required = [
        ("fastapi", "fastapi"),
        ("uvicorn", "uvicorn"),
        ("pika", "pika"),
        ("requests", "requests")
    ]
    missing = []
    for mod, pkg in required:
        try:
            __import__(mod)
        except ImportError:
            missing.append(pkg)
    if missing:
        print("\n[ВНИМАНИЕ] Не установлены необходимые пакеты:")
        for m in missing:
            print(f"  - {m}")
        print(f"Установите их: pip install {' '.join(missing)}\n")

def start_service(name, script_path, env_vars=None):
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONPATH"] = CURRENT_DIR
    if env_vars:
        env.update(env_vars)
    
    print(f"[Запуск] {name}...")
    p = subprocess.Popen(
        [PYTHON_EXE, script_path],
        cwd=CURRENT_DIR,
        env=env
    )
    processes.append((name, p))
    return p

def main():
    print("=" * 70)
    print("  ЗАПУСК ЛАБОРАТОРНОЙ РАБОТЫ №4: ПАТТЕРН SAGA (ХОРЕОГРАФИЯ СОБЫТИЙ)")
    print("  Режим: Локальный запуск микросервисов")
    print("=" * 70)

    check_dependencies()

    # Проверка доступности RabbitMQ
    rabbitmq_online = is_port_in_use(5672)
    if rabbitmq_online:
        print("[RabbitMQ] Обнаружен локальный брокер RabbitMQ на порту 5672 [OK]")
    else:
        print("[RabbitMQ] Брокер на порту 5672 не найден. Сервисы будут использовать встроенную шину событий.")
        print("           (Для запуска полного стека с RabbitMQ используйте: docker compose up --build)")

    # Освобождение сервисных портов
    ports = [8001, 8002, 8003]
    for port in ports:
        if is_port_in_use(port):
            kill_process_on_port(port)

    time.sleep(0.5)

    try:
        # 1. Order Service (порт 8001)
        start_service(
            "Order Service",
            os.path.join(CURRENT_DIR, "order_service", "app.py"),
            {"PORT": "8001"}
        )

        # 2. Payment Service (порт 8002)
        start_service(
            "Payment Service",
            os.path.join(CURRENT_DIR, "payment_service", "app.py"),
            {"PORT": "8002"}
        )

        # 3. Notification Service (порт 8003)
        start_service(
            "Notification Service",
            os.path.join(CURRENT_DIR, "notification_service", "app.py"),
            {"PORT": "8003"}
        )

        time.sleep(2)
        print("\n" + "=" * 70)
        print("  ВСЕ СЕРВИСЫ УСПЕШНО ЗАПУЩЕНЫ!")
        print("=" * 70)
        print("  -> Order Service Dashboard:        http://localhost:8001/")
        print("  -> Order Service Swagger UI:       http://localhost:8001/docs")
        print("  -> Payment Service Swagger UI:     http://localhost:8002/docs")
        print("  -> Notification Service Swagger:   http://localhost:8003/docs")
        if rabbitmq_online:
            print("  -> RabbitMQ Management UI:         http://localhost:15672/ (guest/guest)")
        print("=" * 70)
        print("  Для автоматической проверки запустите: python test_saga.py")
        print("  Для остановки всех сервисов нажмите Ctrl + C")
        print("=" * 70 + "\n")

        while True:
            for name, p in processes:
                if p.poll() is not None:
                    print(f"[ВНИМАНИЕ] Сервис {name} завершился с кодом {p.returncode}")
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nОстановка всех сервисов...")
        for name, p in processes:
            print(f"Останавливаю {name}...")
            p.terminate()
        print("Все сервисы остановлены.")

if __name__ == "__main__":
    main()
