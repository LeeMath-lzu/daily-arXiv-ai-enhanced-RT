#!/usr/bin/env python3
"""
检查Scrapy爬取统计信息的脚本 / Script to check Scrapy crawling statistics
用于获取去重检查的状态结果 / Used to get deduplication check status results

功能说明 / Features:
- 检查当日与昨日论文数据的重复情况 / Check duplication between today's and yesterday's paper data
- 删除重复论文条目，保留新内容 / Remove duplicate papers, keep new content
- 根据去重后的结果决定工作流是否继续 / Decide workflow continuation based on deduplication results

本版改动 / Changes in this version:
- 使用 UTC 日期与工作流保持一致 / Use UTC date to match workflow output filenames
- 去重键改为 (id, version)；历史无 version 视作 v1 / Deduplicate by (id, version); default version=v1 if missing
- section == "repl" 的条目永远保留 / Always keep entries with section == "repl"
- 同日内部也按 (id, version) 去重 / Intra-day dedup by (id, version)
"""
import json
import sys
import os
from datetime import datetime, timedelta
from collections import Counter


def load_papers_data(file_path):
    """
    从jsonl文件中加载完整的论文数据
    Load complete paper data from jsonl file
    
    Args:
        file_path (str): JSONL文件路径 / JSONL file path
        
    Returns:
        list: 论文数据列表 / List of paper data
    """
    if not os.path.exists(file_path):
        return []
    
    papers = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    data = json.loads(line)
                    papers.append(data)
        return papers
    except Exception as e:
        print(f"Error reading {file_path}: {e}", file=sys.stderr)
        return []


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


def key_of(paper):
    """
    生成去重键 (id, version)，若缺失 version 则视为 v1
    Build dedup key (id, version); default version=v1 if missing
    """
    pid = paper.get('id', '')
    ver = paper.get('version') or 'v1'
    return (pid, ver)


def perform_deduplication():
    """
    执行多日去重：删除与历史多日重复的论文条目，保留新内容
    Perform deduplication over multiple past days
    
    Returns:
        str: 去重状态 / Deduplication status
             - "has_new_content": 有新内容 / Has new content
             - "no_new_content": 无新内容 / No new content  
             - "no_data": 无数据 / No data
             - "error": 处理错误 / Processing error
    """
    # 与 workflow 保持一致：使用 UTC 日期
    today_utc = datetime.utcnow().strftime("%Y-%m-%d")
    today_file = f"../data/{today_utc}.jsonl"
    history_days = 7  # 向前追溯几天的数据进行对比 / How many past days to compare

    if not os.path.exists(today_file):
        print("今日数据文件不存在 / Today's data file does not exist", file=sys.stderr)
        return "no_data"

    try:
        today_papers = load_papers_data(today_file)
        print(f"今日论文总数: {len(today_papers)} / Today's total papers: {len(today_papers)}", file=sys.stderr)

        if not today_papers:
            return "no_data"

        # 收集历史多日 (id, version) 集合 / Collect history keys
        history_keys = set()
        for i in range(1, history_days + 1):
            date_str = (datetime.utcnow() - timedelta(days=i)).strftime("%Y-%m-%d")
            history_file = f"../data/{date_str}.jsonl"
            past_papers = load_papers_data(history_file)
            for p in past_papers:
                history_keys.add(key_of(p))

        print(f"历史{history_days}日去重键数量: {len(history_keys)} / History {history_days}-day key count: {len(history_keys)}", file=sys.stderr)

        # 同日内部去重（按 id,version）+ 与历史同版本去重；repl 永远保留
        kept = []
        seen_today = set()
        repl_count = 0
        dup_history_count = 0
        dup_today_count = 0

        for paper in today_papers:
            sec = (paper.get('section') or '').lower()
            k = key_of(paper)

            # Replacements 一律保留 / Always keep replacements
            if sec == 'repl':
                kept.append(paper)
                repl_count += 1
                continue

            # 同日内部去重 / Intra-day dedup
            if k in seen_today:
                dup_today_count += 1
                continue
            seen_today.add(k)

            # 与历史同版本则去掉；版本升级视作新内容
            if k in history_keys:
                dup_history_count += 1
                continue

            kept.append(paper)

        # 打印统计 / Print stats
        by_sec = Counter([(p.get('section') or 'other').lower() for p in kept])
        print(
            f"统计 | kept={len(kept)} repl_kept={repl_count} dup_today={dup_today_count} dup_history={dup_history_count} by_section={dict(by_sec)}",
            file=sys.stderr
        )

        if kept:
            if save_papers_data(kept, today_file):
                print("已更新今日文件（按 (id,version) 去重，并保留 Replacements） / Updated today's file (dedup by (id,version), kept Replacements).", file=sys.stderr)
                return "has_new_content"
            else:
                print("保存去重后的数据失败 / Failed to save deduplicated data", file=sys.stderr)
                return "error"
        else:
            # 与原逻辑一致：若最终为空则删除并视为无新内容
            try:
                os.remove(today_file)
                print("所有论文均为重复内容，已删除今日文件 / All papers are duplicate content, today's file deleted", file=sys.stderr)
            except Exception as e:
                print(f"删除文件失败: {e} / Failed to delete file: {e}", file=sys.stderr)
            return "no_new_content"

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
    
    if dedup_status == "has_new_content":
        print("✅ 去重完成，发现新内容，继续工作流 / Deduplication completed, new content found, continue workflow", file=sys.stderr)
        sys.exit(0)
    elif dedup_status == "no_new_content":
        print("⏹️ 去重完成，无新内容，停止工作流 / Deduplication completed, no new content, stop workflow", file=sys.stderr)
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
