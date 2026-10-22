from dataclasses import dataclass



@dataclass(frozen=False)
class CapChangeParams:
    amount: int
    total_cost: int
    price: int
    old_share: int
    contribution: int
    premium: int
    reserve: int
    @property
    def new_share(self) -> int:
        return  self.old_share + self.contribution + self.premium + self.reserve


def relu(x: int) -> int:
    return max(0, x)


def capital_increase(params: CapChangeParams) -> dict[str, int]:
    new_share = params.new_share
    bonus = int((params.amount * (params.reserve / params.old_share)) // 1)
    ros = int((params.amount * (params.contribution / params.old_share)) // 1)
    cost = (params.total_cost + (1000 * ros)) / (params.amount + bonus + ros)
    cost_share = int(round(cost * params.amount))
    cost_bonus = int(round(cost * bonus))
    cost_ros = params.total_cost - (cost_share + cost_bonus)
    old_cap = params.old_share * params.price
    if params.premium == 0:
        adj_price = int(((old_cap + (1000 * params.contribution)) / new_share) // 1)
    else:
        adj_price = int(((old_cap + (1000 * (params.contribution + params.premium + params.reserve))) / new_share) // 1)
    return {
        "adj_price": adj_price,
        "ros_price": adj_price - 1000,
        "bonus": bonus,
        "ros": ros,
        "cost_share": cost_share,
        "cost_bonus": cost_bonus,
        "cost_ros": cost_ros,
    }
