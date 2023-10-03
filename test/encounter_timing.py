from neo4j_connect import Neo4jConnection

QUERY = \
"""
match (n:{Encounter})
with count(n) as numEnc
match ()-[r:{ENC}_STARTED]->()
with numEnc, count(r) as numStarted
match ()-[r:{ENC}_ENDED]->()
with numEnc, numStarted, count(r) as numEnded
return "{Encounter} timing: " + case when numEnc = numStarted and numEnc = numEnded then "PASSED" else "FAILED" end
"""

PLACE_HOLDERS = ("Encounter", "ENC")
TEST_VECTORS = [("Visit", "VIS"), ("Stop", "STOP")]

def test_encounter_timing():
    conn = Neo4jConnection()
    for vector in TEST_VECTORS:
        param_dict = {}
        for place_holder, value in zip(PLACE_HOLDERS, vector):
            param_dict[place_holder] = value
        records = conn.execute_read_query(QUERY.format(**param_dict))
        for record in records:
            print(record)
    conn.close()

if __name__ == "__main__":
    test_encounter_timing()