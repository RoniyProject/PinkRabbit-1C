"""Turn a pytest JUnit XML report into GitHub Actions annotations and a job summary.

Each failed or broken test becomes an error annotation (visible in the run annotations and
through the public check-runs API), the summary lists every test with its result.
Usage: python3 pytest_report.py pytest-report.xml
"""
import os
import sys
import xml.etree.ElementTree as ET

MAX_ANNOTATIONS = 10
MAX_TEXT = 3000


def escape(text):
    return text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def main(path):
    if not os.path.exists(path):
        print("::error title=pytest::report %s was not created" % path)
        return
    root = ET.parse(path).getroot()
    cases = root.iter("testcase")
    results = []
    for case in cases:
        name = "%s::%s" % (case.get("classname", ""), case.get("name", ""))
        status = "passed"
        text = ""
        for tag in ("failure", "error"):
            node = case.find(tag)
            if node is not None:
                status = tag
                text = (node.get("message") or "") + "\n" + (node.text or "")
        if case.find("skipped") is not None:
            status = "skipped"
        results.append((name, status, text.strip()))

    failed = [r for r in results if r[1] in ("failure", "error")]
    counts = {}
    for _, status, _ in results:
        counts[status] = counts.get(status, 0) + 1
    print("::notice title=pytest::%s" % ", ".join("%s %d" % item for item in sorted(counts.items())))
    if failed:
        print("::error title=pytest failed::%s" % escape("\n".join(name for name, _, _ in failed)))
    for name, status, text in failed[:MAX_ANNOTATIONS - 1]:
        print("::error title=%s::%s" % (escape(name), escape(text[:MAX_TEXT])))

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as out:
            out.write("## pytest: %s\n\n" % ", ".join("%s %d" % item for item in sorted(counts.items())))
            for name, status, text in results:
                out.write("- `%s` %s\n" % (name, status))
            for name, status, text in failed:
                out.write("\n### %s (%s)\n\n```\n%s\n```\n" % (name, status, text[:MAX_TEXT]))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "pytest-report.xml")
