from nba_api.live.nba.endpoints import playbyplay

game_id = "0022400001"

summary = playbyplay.PlayByPlay(game_id=game_id)
actions = summary.get_dict()["game"]["actions"]
fouls = [
    {
        "period": a["period"],
        "clock": a["clock"],
        "team": a.get("teamTricode"),
        "fouler": a.get("playerName"),
        "fouler_id": a.get("personId"),
        "drawn_by": a.get("foulDrawnPlayerName"),
        "drawn_id": a.get("foulDrawnPersonId"),
        "subtype": a.get("subType"),
        "descriptor": a.get("descriptor"),
        "description": a.get("description"),
    }
    for a in actions if a.get("actionType") == "foul"
]

for f in fouls:
    print(f)
