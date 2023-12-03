from exceptions import *

def process(data):
    enrichment = {
                "locations": [
                    {
                        "long": 34.4358623,
                        "latitude": 31.5045303,
                        "neighborhood": "Gaza",
                        "name_en": "Galilee (Gaza City)",
                        "name_ar": "الجليل"
                    }
                ]
            }
    
    #raise IrrecoverableException
    #raise ResourceUnavailableException
    
    return enrichment
