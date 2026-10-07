from quality_rules import is_valid_order, validate_order


def test_valid_order():
    order = {
        "order_id": "O1001",
        "customer_id": "C001",
        "quantity": 2,
        "order_amount": 120.50,
    }

    assert is_valid_order(order)
    assert validate_order(order) is None


def test_missing_order_id():
    order = {
        "order_id": None,
        "customer_id": "C001",
        "quantity": 2,
        "order_amount": 120.50,
    }

    assert validate_order(order) == "Missing order_id"


def test_missing_customer_id():
    order = {
        "order_id": "O1001",
        "customer_id": None,
        "quantity": 2,
        "order_amount": 120.50,
    }

    assert validate_order(order) == "Missing customer_id"


def test_invalid_quantity():
    order = {
        "order_id": "O1001",
        "customer_id": "C001",
        "quantity": 0,
        "order_amount": 120.50,
    }

    assert validate_order(order) == "Quantity must be greater than zero"


def test_negative_order_amount():
    order = {
        "order_id": "O1001",
        "customer_id": "C001",
        "quantity": 1,
        "order_amount": -10.00,
    }

    assert validate_order(order) == "Order amount cannot be negative"
