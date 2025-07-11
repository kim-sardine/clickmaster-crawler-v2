import json
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
import tempfile
from datetime import datetime


def ensure_directory_exists(directory_path: str) -> Path:
    """디렉토리가 존재하지 않으면 생성"""
    path = Path(directory_path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def create_temp_file(suffix: str = ".jsonl", prefix: str = "temp_") -> str:
    """임시 파일 생성"""
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=suffix, prefix=prefix, delete=False, encoding="utf-8")
    temp_file.close()
    return temp_file.name


def write_jsonl_file(data: List[Dict[str, Any]], file_path: str) -> int:
    """JSONL 파일 작성"""
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            for item in data:
                json.dump(item, f, ensure_ascii=False)
                f.write("\n")
        return len(data)
    except Exception as e:
        raise Exception(f"JSONL 파일 작성 실패: {str(e)}")


def read_jsonl_file(file_path: str) -> List[Dict[str, Any]]:
    """JSONL 파일 읽기"""
    data = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    data.append(json.loads(line))
        return data
    except Exception as e:
        raise Exception(f"JSONL 파일 읽기 실패: {str(e)}")


def write_json_file(data: Dict[str, Any], file_path: str) -> None:
    """JSON 파일 작성"""
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        raise Exception(f"JSON 파일 작성 실패: {str(e)}")


def read_json_file(file_path: str) -> Dict[str, Any]:
    """JSON 파일 읽기"""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        raise Exception(f"JSON 파일 읽기 실패: {str(e)}")


def delete_file_safely(file_path: str) -> bool:
    """파일 안전 삭제"""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            return True
        return False
    except Exception:
        return False


def get_file_size(file_path: str) -> int:
    """파일 크기 반환 (바이트)"""
    try:
        return os.path.getsize(file_path)
    except Exception:
        return 0


def get_file_age_hours(file_path: str) -> float:
    """파일 생성 후 경과 시간 (시간 단위)"""
    try:
        if not os.path.exists(file_path):
            return 0

        creation_time = os.path.getctime(file_path)
        current_time = datetime.now().timestamp()
        age_seconds = current_time - creation_time

        return age_seconds / 3600
    except Exception:
        return 0


def format_file_size(size_bytes: int) -> str:
    """파일 크기를 읽기 쉬운 형태로 변환"""
    if size_bytes == 0:
        return "0B"

    size_names = ["B", "KB", "MB", "GB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1

    return f"{size_bytes:.1f}{size_names[i]}"


def create_backup_filename(original_path: str) -> str:
    """백업 파일명 생성"""
    path = Path(original_path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{path.stem}_backup_{timestamp}{path.suffix}"
    return str(path.parent / backup_name)


def validate_file_permissions(file_path: str) -> Dict[str, bool]:
    """파일 권한 확인"""
    permissions = {"exists": False, "readable": False, "writable": False, "executable": False}

    try:
        if os.path.exists(file_path):
            permissions["exists"] = True
            permissions["readable"] = os.access(file_path, os.R_OK)
            permissions["writable"] = os.access(file_path, os.W_OK)
            permissions["executable"] = os.access(file_path, os.X_OK)
    except Exception:
        pass

    return permissions


def count_lines_in_file(file_path: str) -> int:
    """파일의 라인 수 계산"""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return sum(1 for _ in f)
    except Exception:
        return 0


def generate_unique_filename(base_path: str, extension: str = "") -> str:
    """중복되지 않는 파일명 생성"""
    counter = 1
    base = Path(base_path)

    if extension and not extension.startswith("."):
        extension = "." + extension

    original_path = f"{base}{extension}"

    while os.path.exists(original_path):
        path_without_ext = f"{base}_{counter}"
        original_path = f"{path_without_ext}{extension}"
        counter += 1

    return original_path
