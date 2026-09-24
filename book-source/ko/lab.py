#!/usr/bin/env python3
"""Reconstruct and check the bundled Wickle 0.2.0 teaching checkpoints."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

COURSE = Path(__file__).resolve().parent
DATA = json.loads((COURSE / 'course.json').read_text(encoding='utf-8'))


def stage(number):
    return next(item for item in DATA['stages'] if item['number'] == number)


def run(command, cwd):
    print('+ ' + ' '.join(map(str, command)), flush=True)
    subprocess.run(command, cwd=cwd, check=True)


def patch_path(item):
    path = COURSE / item['patch']
    if hashlib.sha256(path.read_bytes()).hexdigest() != item['patch_sha256']:
        raise ValueError('Patch checksum mismatch: ' + str(path))
    return path


def differences(item, work):
    return [name for name, digest in item['files'].items()
            if not (work / name).is_file()
            or hashlib.sha256((work / name).read_bytes()).hexdigest() != digest]


def snapshot(number, destination):
    # Never overwrite a learner's implementation, even an existing empty folder.
    if destination.exists():
        raise ValueError('Destination must not exist; choose a new directory.')
    selected = [s for s in DATA['stages'] if s['number'] <= number]
    patches = [patch_path(s) for s in selected]
    destination.mkdir(parents=True)
    run(['git', 'init', '-q', str(destination)], COURSE)
    for patch in patches:
        run(['git', 'apply', '--check', str(patch)], destination)
        run(['git', 'apply', str(patch)], destination)
    missing = differences(stage(number), destination)
    if missing:
        raise ValueError('Snapshot differs: ' + ', '.join(missing))
    print(f'PASS: checkpoint {number:02d}, {len(stage(number)["files"])} exact files')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('list')
    for name in ['snapshot', 'inspect', 'compare', 'check']:
        part = sub.add_parser(name)
        part.add_argument('chapter', type=int, choices=[s["number"] for s in DATA["stages"]])
        if name == 'snapshot':
            part.add_argument('--dest', type=Path, required=True)
        elif name in ['compare', 'check']:
            part.add_argument('--work', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'list':
        for s in DATA['stages']:
            print(f'{s["number"]:02d} {s["title"]}')
        return
    item = stage(args.chapter)
    if args.command == 'snapshot':
        snapshot(args.chapter, args.dest.resolve())
    elif args.command == 'inspect':
        print(item['title'])
        print('Answer patch:', patch_path(item))
        print('Changed files:\n' + '\n'.join(item['changed']))
    else:
        work = args.work.resolve()
        if not (work / 'Cargo.toml').is_file():
            raise ValueError('Expected an exercise workspace with Cargo.toml.')
        if args.command == 'compare':
            diff = differences(item, work)
            print('\n'.join(diff) if diff else 'PASS: all reference files match byte for byte')
            print('Additional learner files are not compared; correctness also requires tests.')
            if diff:
                sys.exit(1)
        else:
            for command in item['commands']:
                run(command, work)
            print('PASS: chapter tests; not a substitute for the final workspace/package checks.')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
