"""Deprecated legacy entry point.

Selection and reporting are deliberately separate in v2 so test results can
never influence parameter choice.
"""


def main():
    raise SystemExit(
        "evaluation/select_and_report.py is deprecated. "
        "Use evaluation/select_params.py for train→validation selection, "
        "then evaluation/final_report.py for reporting."
    )


if __name__ == "__main__":
    main()
