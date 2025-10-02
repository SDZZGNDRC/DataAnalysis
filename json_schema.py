import json
import sys

def generate_schema(data):
    """
    递归生成JSON数据的schema。
    """
    if isinstance(data, dict):
        properties = {}
        for key, value in data.items():
            properties[key] = generate_schema(value)
        return {"type": "object", "properties": properties}
    elif isinstance(data, list):
        if not data:
            return {"type": "array", "items": {}}
        # 假设列表中所有元素类型相同，取第一个元素的schema
        item_schema = generate_schema(data[0])
        return {"type": "array", "items": item_schema}
    elif isinstance(data, str):
        return {"type": "string"}
    elif isinstance(data, int):
        return {"type": "integer"}
    elif isinstance(data, float):
        return {"type": "number"}
    elif isinstance(data, bool):
        return {"type": "boolean"}
    elif data is None:
        return {"type": "null"}
    else:
        # 对于其他类型，回退为string
        return {"type": "string"}

def main(json_file_path):
    """
    指定JSON文件路径，读取并输出其schema。
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        schema = generate_schema(data)
        print(json.dumps(schema, indent=2, ensure_ascii=False))
    except FileNotFoundError:
        print(f"文件未找到: {json_file_path}")
    except json.JSONDecodeError as e:
        print(f"JSON解析错误: {e}")
    except Exception as e:
        print(f"发生错误: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("用法: python json_schema.py <json_file_path>")
    else:
        main(sys.argv[1])