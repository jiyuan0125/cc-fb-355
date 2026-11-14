#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""repartition.py — 按月业务日期重整 partitions 分区目录。

流程（顺序固定）:
  1. 归位: 每行按业务日期列(order_date)所属自然月重新归类, 与文件所在物理目录无关;
  2. 裁剪: 业务日期早于保留窗口的行整体丢弃(to_dir=dropped), 过期月整目录删除。
     保留窗口自业务当月起向前数 retain_months 个整月(含业务当月);
  3. 合并: 归位裁剪后, 行数 < target_rows 的月目录与相邻月合并。
     承接方取相邻月中余量(target_rows - 当前行数)最大者; 余量相同取更早的月。
     合并后目录以承接方月份命名, 被并入目录删除; 循环至无小目录或仅剩一个目录。

输出:
  move_log.csv  表头 pk,from_dir,to_dir; 仅记录发生变迁(含 dropped)的行, 按 pk 升序;
  counts.txt    global_before/global_after/global_dropped, 随后 dir_before(覆盖重整前
                全部目录) 与 dir_after 各月行数; global_before = global_after + global_dropped。

用法:
  python3 -I repartition.py [partitions_dir] [policy_file] [--today YYYY-MM-DD]
                            [--date-col order_date]
不传路径时默认 ./partitions 与 ./policy.txt, 业务日默认 2026-11-14。
"""

import argparse
import calendar
import csv
import os
import re
import sys
from collections import defaultdict
from datetime import date

MONTH_DIR_RE = re.compile(r"^\d{4}-\d{2}$")
OUT_CSV_NAME = "orders.csv"


def shift_month(year, month, delta):
    """返回 (year, month) 向前(负)或向后(正)平移 delta 个自然月。"""
    index = year * 12 + (month - 1) + delta
    return index // 12, index % 12 + 1


def month_last_day(year, month):
    return calendar.monthrange(year, month)[1]


def load_policy(path):
    """policy.txt 两行: 支持 'key value' 或纯数字(按行序 retain_months, target_rows)。"""
    retain_months = None
    target_rows = None
    bare = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            tokens = line.split()
            if not tokens:
                continue
            if tokens[0] == "retain_months":
                retain_months = int(tokens[1])
            elif tokens[0] == "target_rows":
                target_rows = int(tokens[1])
            else:
                bare.append(int(tokens[-1]))
    if retain_months is None:
        retain_months = bare[0]
    if target_rows is None:
        target_rows = bare[1]
    if retain_months <= 0 or target_rows <= 0:
        raise ValueError("retain_months 与 target_rows 必须为正整数")
    return retain_months, target_rows


def read_partitions(partitions_dir, date_col):
    """读取所有月目录下的 csv。

    返回:
      rows: [(from_dir, row_dict, biz_month 'YYYY-MM', biz_date str), ...]
      before_counts: {from_dir: 行数} 覆盖重整前全部目录
      header: 原始表头
    """
    rows = []
    before_counts = {}
    header = None
    for month_dir in sorted(os.listdir(partitions_dir)):
        dir_path = os.path.join(partitions_dir, month_dir)
        if not (os.path.isdir(dir_path) and MONTH_DIR_RE.match(month_dir)):
            continue
        csv_paths = sorted(
            p for p in os.listdir(dir_path) if p.lower().endswith(".csv")
        )
        dir_total = 0
        for csv_name in csv_paths:
            with open(os.path.join(dir_path, csv_name), "r", encoding="utf-8",
                      newline="") as fh:
                reader = csv.DictReader(fh)
                if header is None:
                    header = reader.fieldnames
                for row in reader:
                    biz_date = row[date_col].strip()
                    rows.append((month_dir, row, biz_date[:7], biz_date))
                    dir_total += 1
        before_counts[month_dir] = dir_total
    if header is None:
        raise RuntimeError("partitions 目录下未找到任何 csv 数据")
    return rows, before_counts, header


def plan_merges(group_counts, target_rows):
    """输入 {月: 行数}(仅保留窗口内), 返回 {月: 最终承接月}。"""
    groups = dict(group_counts)
    owner = {month: month for month in groups}

    while len(groups) > 1:
        months = sorted(groups)
        small = [m for m in months if groups[m] < target_rows]
        if not small:
            break
        donor = small[0]
        i = months.index(donor)
        neighbors = []
        if i > 0:
            neighbors.append(months[i - 1])
        if i + 1 < len(months):
            neighbors.append(months[i + 1])
        # 余量 = target - 行数, 越大越能承接; 平局取更早月(sorted 后 max 保留首个)
        receiver = max(
            sorted(neighbors),
            key=lambda m: target_rows - groups[m],
        )
        groups[receiver] += groups[donor]
        del groups[donor]
        for src_month, own in owner.items():
            if own == donor:
                owner[src_month] = receiver
    return owner


def write_partitions(partitions_dir, final_groups, header):
    """删除旧月目录, 按最终分组重写。"""
    for name in os.listdir(partitions_dir):
        path = os.path.join(partitions_dir, name)
        if os.path.isdir(path) and MONTH_DIR_RE.match(name):
            for child in os.listdir(path):
                os.remove(os.path.join(path, child))
            os.rmdir(path)
    for month in sorted(final_groups):
        os.makedirs(os.path.join(partitions_dir, month), exist_ok=True)
        with open(os.path.join(partitions_dir, month, OUT_CSV_NAME), "w",
                  encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=header)
            writer.writeheader()
            writer.writerows(final_groups[month])


def main():
    parser = argparse.ArgumentParser(description="按月业务日期重整分区")
    parser.add_argument("partitions_dir", nargs="?", default="partitions")
    parser.add_argument("policy_file", nargs="?", default="policy.txt")
    parser.add_argument("--today", default="2026-11-14",
                        help="业务当日 YYYY-MM-DD, 默认 2026-11-14")
    parser.add_argument("--date-col", default="order_date",
                        help="业务日期列名, 默认 order_date")
    args = parser.parse_args()

    today = date.fromisoformat(args.today)
    retain_months, target_rows = load_policy(args.policy_file)

    # 保留窗口: [start_month, today.month], 裁剪边界为 start_month 上一月的最后一天(含当天)
    start_year, start_month = shift_month(
        today.year, today.month, -(retain_months - 1)
    )
    boundary_year, boundary_month = shift_month(start_year, start_month, -1)
    cutoff = date(
        boundary_year, boundary_month,
        month_last_day(boundary_year, boundary_month),
    )
    cutoff_str = cutoff.isoformat()
    start_month_str = "%04d-%02d" % (start_year, start_month)

    rows, before_counts, header = read_partitions(
        args.partitions_dir, args.date_col
    )

    # 1) 归位 + 2) 裁剪: 按业务月分组, 过期行计入 dropped
    retained = defaultdict(list)   # biz_month -> [row_dict, ...]
    dropped_count = 0
    transitions = []               # (pk, from_dir, to_dir)
    for from_dir, row, biz_month, biz_date in rows:
        if biz_date <= cutoff_str:
            dropped_count += 1
            transitions.append((row["pk"], from_dir, "dropped"))
        else:
            retained[biz_month].append(row)

    # 3) 合并小目录
    group_counts = {m: len(v) for m, v in retained.items()}
    owner = plan_merges(group_counts, target_rows)

    final_groups = defaultdict(list)
    for biz_month, row_list in retained.items():
        final_groups[owner[biz_month]].extend(row_list)

    # 变迁日志: 仅记录最终去向与原物理目录不同的行, 按 pk 升序
    for from_dir, row, biz_month, biz_date in rows:
        if biz_date <= cutoff_str:
            continue  # dropped 已记录
        to_dir = owner[biz_month]
        if to_dir != from_dir:
            transitions.append((row["pk"], from_dir, to_dir))
    transitions.sort(key=lambda item: item[0])

    base_dir = os.path.dirname(os.path.abspath(args.partitions_dir))
    with open(os.path.join(base_dir, "move_log.csv"), "w", encoding="utf-8",
              newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["pk", "from_dir", "to_dir"])
        writer.writerows(transitions)

    global_before = len(rows)
    global_after = sum(len(v) for v in final_groups.values())

    with open(os.path.join(base_dir, "counts.txt"), "w", encoding="utf-8") as fh:
        fh.write("global_before %d\n" % global_before)
        fh.write("global_after %d\n" % global_after)
        fh.write("global_dropped %d\n" % dropped_count)
        for month in sorted(before_counts):
            fh.write("dir_before %s %d\n" % (month, before_counts[month]))
        for month in sorted(final_groups):
            fh.write("dir_after %s %d\n" % (month, len(final_groups[month])))

    # 重写分区目录
    write_partitions(args.partitions_dir, final_groups, header)

    # 守恒校验
    if global_before != global_after + dropped_count:
        raise RuntimeError("行数不守恒: %d != %d + %d"
                           % (global_before, global_after, dropped_count))
    if sum(before_counts.values()) != global_before:
        raise RuntimeError("dir_before 合计与 global_before 不一致")

    print("业务当日: %s  保留窗口: %s 起  裁剪边界(含): %s"
          % (args.today, start_month_str, cutoff_str))
    print("global_before=%d global_after=%d global_dropped=%d"
          % (global_before, global_after, dropped_count))
    print("最终目录: %s"
          % ", ".join("%s=%d" % (m, len(final_groups[m]))
                      for m in sorted(final_groups)))
    print("move_log 变迁行数: %d" % len(transitions))


if __name__ == "__main__":
    sys.exit(main())
