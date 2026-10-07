def validate_order(order):
    if not order.get("order_id"):
        return "Missing order_id"

    if not order.get("customer_id"):
        return "Missing customer_id"

    if order.get("quantity") is None or order["quantity"] <= 0:
        return "Quantity must be greater than zero"

    if order.get("order_amount") is None or order["order_amount"] < 0:
        return "Order amount cannot be negative"

    return None


def is_valid_order(order):
    return validate_order(order) is None
