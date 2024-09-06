from ops_notion import OperatorNotion


def apply():
    print("Initializing Notion database tables (Reddit) ...")
    op = OperatorNotion()

    return True, "OK"
