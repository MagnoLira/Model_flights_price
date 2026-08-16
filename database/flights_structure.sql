"""
municipio: city that airport is located
aeroporto: airport name
iata_code: airport code (usually 3 -letters, e.g : GRU)
"""
create table flights.aeroportos_loc(
municipio text, 
aeroporto text, 
iata_code text 
)




"""
aer_de: origin airport (iata_code),
aer_para: destity airport (iata_code)
"""

create table flights.aeroportos_config(
aer_de text,
aer_para text
)

