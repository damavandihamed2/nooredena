import json

id_file_map = [
    {'stat_id': 28579, 'save_name': "consumption_index", 'publish_date': None},
    {'stat_id': 52141, 'save_name': "production_index", 'publish_date': None},
    {'stat_id': 22428, 'save_name': "mean_price_index", 'publish_date': None},
    {'stat_id': 57092, 'save_name': "building_index", 'publish_date': None},
    {'stat_id': 22572, 'save_name': "unemployment_rate", 'publish_date': None},
    {'stat_id': 22569, 'save_name': "participation_rate", 'publish_date': None},
    {'stat_id': 54306, 'save_name': "national_accounts", 'publish_date': None}
]

initial_data = {str(item['stat_id']): item for item in id_file_map}

with open("amar_center/config.json", "w", encoding="utf-8") as f:
    json.dump(initial_data, f, ensure_ascii=False, indent=4)
