from utils.get_token import get_battle_net_access_token
from utils.get_league_data import league_data


acesso = get_battle_net_access_token(client_id='14fd2a97eefe4115a3aa2e64e437b6bb',client_secret='JzzXOrIV7pt5vn1iEZ5Iidw6AgvCTyst')
abc = league_data(season_id=65,queue_id=201,team_type=0,league_id=4,token=acesso)

print(abc)