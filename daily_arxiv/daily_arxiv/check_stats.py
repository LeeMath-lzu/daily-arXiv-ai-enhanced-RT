#!/usr/bin/env python3
"""
检查Scrapy爬取统计信息的脚本 / Script to check Scrapy crawling statistics
用于获取去重检查的状态结果 / Used to get deduplication check status results

功能说明 / Features:
- 检查当日论文数据是否存在 / Check whether today's paper data exists
- 不做跨日期去重，完整保留当天抓取的论文 / Do not deduplicate across days; keep all papers crawled today
- 根据检查结果决定后续工作流是否继续 / Decide workflow continuation based on check result
"""
import json
import sys
import os
from datetime import datetime, timedelta


def load_papers_data(file_path):
    """
    从jsonl文件中加载完整的论文数据
    Load complete paper data from jsonl file

    Args:
        file_path (str): JSONL文件路径 / JSONL file path

    Returns:
        list: 论文数据列表 / List of paper data
        set: 论文ID集合 / Set of paper IDs
    """
    if not os.path.exists(file_path):
        return [], set()

    papers = []
    ids = set()
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    data = json.loads(line)
                    papers.append(data)
                    ids.add(data.get('id', ''))
        return papers, ids
    except Exception as e:
        print(f"Error reading {file_path}: {e}", file=sys.stderr)
        return [], set()


def save_papers_data(papers, file_path):
    """
    保存论文数据到jsonl文件
    Save paper data to jsonl file

    Args:
        papers (list): 论文数据列表 / List of paper data
        file_path (str): 文件路径 / File path
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            for paper in papers:
                f.write(json.dumps(paper, ensure_ascii=False) + '\n')
        return True
    except Exception as e:
        print(f"Error saving {file_path}: {e}", file=sys.stderr)
        return False


def perform_deduplication():
    """
    不再做跨日期去重，只检查今天是否有数据。
    Do not perform cross-day deduplication; only check whether today's data exists.

    返回值 / Return:
        str: 去重状态 / Deduplication status
             - "has_new_content": 有新内容 / Has new content
             - "no_new_content": 无新内容（当前逻辑不会返回该值）/ No new content (not used currently)
             - "no_data": 无数据 / No data
             - "error": 处理错误 / Processing error
    """
    today = datetime.now().strftime("%Y-%m-%d")
    today_file = f"../data/{today}.jsonl"

    if not os.path.exists(today_file):
        print("今日数据文件不存在 / Today's data file does not exist", file=sys.stderr)
        return "no_data"

    try:
        today_papers, today_ids = load_papers_data(today_file)
        print(f"今日论文总数: {len(today_papers)} / Today's total papers: {len(today_papers)}", file=sys.stderr)

        if not today_papers:
            # 有文件但没有任何论文
            print("今日文件存在但没有任何论文记录 / Today's file exists but contains no papers",
                  file=sys.stderr)
            return "no_data"

        # 关键点：不再进行跨日期 ID 去重，以避免删除 arXiv Replacements 或 cross-list 论文。
        # Key point: skip cross-day ID deduplication to avoid dropping arXiv replacements or cross-lists.
        print(
            "不执行跨日期去重，保留所有 new/cross/replacement 论文 / "
            "Skip cross-day dedup, keep all new/cross/replacement papers",
            file=sys.stderr,
        )
        return "has_new_content"

    except Exception as e:
        print(f"去重处理失败: {e} / Deduplication processing failed: {e}", file=sys.stderr)
        return "error"


def main():
    """
    检查去重状态并返回相应的退出码
    Check deduplication status and return corresponding exit code

    退出码含义 / Exit code meanings:
    0: 有新内容，继续处理 / Has new content, continue processing
    1: 无新内容，停止工作流 / No new content, stop workflow
    2: 处理错误 / Processing error
    """

    print("正在执行去重检查... / Performing intelligent deduplication check...", file=sys.stderr)

    # 执行去重处理 / Perform deduplication processing
    dedup_status = perform_deduplication()

    # 根据返回状态决定退出码 / Set exit code based on status
    if dedup_status == "has_new_content":
        print("✅ 有新内容，继续后续处理 / Has new content, continue workflow", file=sys.stderr)
        sys.exit(0)
    elif dedup_status == "no_new_content":
        print("ℹ️ 今日无新增论文，停止后续处理 / No new papers today, stop workflow", file=sys.stderr)
        sys.exit(1)
    elif dedup_status == "no_data":
        print("⏹️ 今日无数据，停止工作流 / No data today, stop workflow", file=sys.stderr)
        sys.exit(1)
    elif dedup_status == "error":
        print("❌ 去重处理出错，停止工作流 / Deduplication processing error, stop workflow", file=sys.stderr)
        sys.exit(2)
    else:
        # 意外情况：未知状态 / Unexpected case: unknown status
        print("❌ 未知去重状态，停止工作流 / Unknown deduplication status, stop workflow", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
