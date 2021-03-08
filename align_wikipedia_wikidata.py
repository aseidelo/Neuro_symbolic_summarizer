import argparse
import time
import random
import re
import json
import requests
from datetime import datetime
from dateutil.parser import parse
from nltk import ngrams, word_tokenize
from nltk.stem import WordNetLemmatizer 
import datefinder # to find dates on text
import inflect # to find numbers on text
from lat_lon_parser import to_str_deg_min_sec # to find coordinates on text

def chunks(lst, n):
	"""Yield successive n-sized chunks from lst."""
	for i in range(0, len(lst), n):
		yield lst[i:i + n]

def load_wiki_txts(file_path):
	wiki_pages_dict = {}
	titles = []
	with open(file_path, 'r') as file:
		for line in file:
			article = json.loads(line)
			wiki_pages_dict.update(article)
			for title in article:
				titles.append(title)
	return titles, wiki_pages_dict

def load_wiki_web_txts(wiki_file_path, web_file_path):
	wiki_pages_dict = {}
	titles = []
	with open(wiki_file_path, 'r') as file:
		for line in file:
			article = json.loads(line)
			wiki_pages_dict.update(article)
			for title in article:
				titles.append(title)
	web_pages_dict = {}
	with open(web_file_path, 'r') as file:
		i = 0
		for line in file:
			web_pages = json.loads(line)
			paragraphs = []
			for page in web_pages:
				paragraphs = paragraphs + web_pages[page]
			web_pages_dict[titles[i]] = paragraphs
			i = i + 1
	return titles, wiki_pages_dict, web_pages_dict

def list2str(vals):
	txtlist = ""
	for i in range(len(vals)):
		txtlist = txtlist + vals[i]
		if(i < len(vals) - 1):
			txtlist = txtlist + '|'
	return txtlist

def get_items_names_and_aliases(ids):
	ids_txt = list2str(ids)
	url = "https://www.wikidata.org/w/api.php?action=wbgetentities&props=labels|aliases&ids={}&languages=en&format=json".format(ids_txt)
	res = requests.get(url)
	response = res.json()
	if('entities' in response):
		ids = []
		for id in response['entities']:
			if('Q' in id):
				ids.append(id)
		entities_names = {}
		for id in ids:
			if('en' in response['entities'][id]['labels']):
				label = response['entities'][id]['labels']['en']['value']
				names = [label.lower()]
				try:
					aliases = response['entities'][id]['aliases']['en']
					for alias in aliases:
						names.append(alias['value'].lower())
				except:
					pass
				entities_names.update({id:names})
		return entities_names
	else:
		return None

def process_relation_datavalue(relation_datavalue):
	datavalue_type = relation_datavalue['type']
	datavalue_value = relation_datavalue['value']
	value = None
	if(datavalue_type == 'time'):
		value = datavalue_value['time']
	elif(datavalue_type == 'wikibase-entityid'):
		value = datavalue_value['id']
	elif(datavalue_type == 'quantity'):
		value = datavalue_value
	elif(datavalue_type == 'globecoordinate'):
		value = datavalue_value
	if(value is not None):
		return datavalue_type, value
	else:
		return None, None


def get_titles_aliases_relations_related_entities(titles):
	try:
		titles_txt = list2str(titles)
		url = "https://www.wikidata.org/w/api.php?action=wbgetentities&sites=enwiki&titles={}&props=labels|claims|aliases|sitelinks&languages=en&format=json".format(titles_txt)
		res = requests.get(url)
		response = res.json()
		ids = []
		for id in response['entities']:
			if('Q' in id):
				ids.append(id)
		print(ids)
		entities_relations = {}
		entities_names = {}
		for id in ids:
			#print(id)
			relations = {}
			label = response['entities'][id]['labels']['en']['value']
			title = response['entities'][id]['sitelinks']['enwiki']['title']
			names = [label.lower(), title.lower()]
			try:
				aliases = response['entities'][id]['aliases']['en']
				for alias in aliases:
					names.append(alias['value'])
			except:
				pass
			entities_names.update({id:names})
			related_entities = []
			for claim_name in response['entities'][id]['claims']:
				claim_datas = response['entities'][id]['claims'][claim_name]
				relations[claim_name] = []
				for claim_data in claim_datas:
					if(claim_data['type'] == "statement"):
						if('datavalue' in claim_data['mainsnak']):
							value_type, value = process_relation_datavalue(claim_data['mainsnak']['datavalue'])
							if(value_type == 'wikibase-entityid'):
								related_entities.append(value)
							elif(value_type == 'quantity'):
								if('http://www.wikidata.org/entity/' in value['unit']):
									unit_entity = value['unit'].split('/')[-1]
									related_entities.append(unit_entity)
									value['unit'] = unit_entity
							qualifiers = {}
							if('qualifiers' in claim_data):
								for qualifier_relation_name in claim_data['qualifiers']:
									qualifiers[qualifier_relation_name] = []
									qualifier_relation_values = claim_data['qualifiers'][qualifier_relation_name]
									for qualifier_data in qualifier_relation_values:
										if('datavalue' in qualifier_data):
											qualifier_type, qualifier_value = process_relation_datavalue(qualifier_data['datavalue'])
											if(qualifier_type is not None):
												qualifiers[qualifier_relation_name].append({'type' : qualifier_type, 'value' : qualifier_value})
												if(qualifier_type == 'wikibase-entityid'):
													related_entities.append(qualifier_value)
							if(value_type is not None):
								relations[claim_name].append({'type' : value_type, 'value' : value, 'qualifiers': qualifiers})
				#print(relations[claim_name])
			entity_names_to_search = []
			for entity in related_entities:
				if(entity not in entities_names):
					entity_names_to_search.append(entity)
			related_entities_names = get_items_names_and_aliases(entity_names_to_search)
			if(related_entities_names is not None):
				entities_names.update(related_entities_names)
			entities_relations[title] = {'id' : id, 'names' : names, 'relations' : relations}
		#for entity_id in entities:
		#	print(entity_id)
		#	print(entities[entity_id]['relations'])
		return entities_names, entities_relations
	except:
		return {},{}

def preprocess_sentence(w, tags=False):
	# criando um espaco entre palavras e pontuacoes
	# ex: "meu nome e Andre." => "meu nome e Andre ."
	w = re.sub(r"([?.!,¿])", r" \1 ", w)
	w = re.sub(r'[" "]+', " ", w)
	# substituir tudo por espaço exceto (a-z, A-Z, ".", "?", "!", ",", letras com acentos da lingua pt)
	# e depois colocar em caixa baixa
	w = re.sub(r"[^a-zA-Z0-9çÇéêíáâãõôóúûÉÊÍÁÂÃÕÔÓÚÛ?.!,¿]+", " ", w).lower()
	w = w.strip()
	# adicionar SOS (inicio) e EOS nas saídas
	if(tags):
		w = "<SOS> " + w + " <EOS>"
	return w

def process_wiki_sections(sections_titles, sections):
	paragraphs = []
	titles = []
	for i in range(len(sections)): #section in sections:
		section = sections[i]
		section_title = sections_titles[i]
		section_sentences = section.split('\n')
		for j in range(len(section_sentences)):
			titles.append(section_title)
		for sentence in section_sentences:
			if(len(sentence) != 0):
				paragraphs.append(preprocess_sentence(sentence))
	return paragraphs, titles

def process_web_paragraphs(paragraphs):
	processed_paragraphs = []
	for sentence in paragraphs: #section in sections:
		processed_paragraphs.append(preprocess_sentence(sentence))
	return processed_paragraphs

def lemmatize_paragraph(lemmatizer, paragraph):
	punctuations="?:!.,;"
	paragraph_words = word_tokenize(paragraph)
	for word in paragraph_words:
		if word in punctuations:
			paragraph_words.remove(word)
	lemma_para = ''
	for word in paragraph_words:
		lemma_para = lemma_para + lemmatizer.lemmatize(word, pos="v") + ' '
	return lemma_para

def find_entity_relation_match(relation_entity, entities_names, paragraph):
	#print("Possible names: {}".format(entities_names[relation_entity]))
	if(relation_entity in entities_names):
		for name in entities_names[relation_entity]:
			'''
			n = len(name.split(' '))
			ngrams_generator = ngrams(paragraph.split(), n)
			paragraph_ngrams = []
			for grams in ngrams_generator:
				paragraph_ngrams.append(grams)
			print(paragraph_ngrams)
			'''
			pos = paragraph.find(' ' + name + ' ')
			if(pos != -1):
				return name, pos
	return None, None

def find_time_relation_match(relation_value, paragraph, wikipedia_dates):
	wikidata_date = None
	try:
		wikidata_date = parse(relation_value[1:], ignoretz=True)
	except ValueError:
		wikidata_date = datetime(int(relation_value[1:5]), 3, 10)
	finally:
		#print(wikidata_date)
		for wikipedia_date, date_pos in wikipedia_dates:
			try:
				if(wikidata_date == wikipedia_date):
					if(wikipedia_date.hour == 0 and wikipedia_date.minute == 0 and wikipedia_date.second == 0):
						return wikidata_date.strftime('%Y / %m / %d'), date_pos
					else:
						return wikidate_date.strftime('%Y / %m / %d %H : %M : %S'), date_pos
				elif(wikipedia_date.month == 2 and wikipedia_date.day == 10): # caso em que nao se sabe mes e dia
					if(wikipedia_date.year == wikidata_date.year):
						return str(wikidata_date.year), date_pos
			except:
				pass
	return None, None

def generate_quantity_names(amount, inflection_engine):
	quantity_names = [amount, amount.replace('-', '').replace('+', '')]
	number_amount = float(amount)
	if(number_amount.is_integer()):
		number_amount = int(number_amount)
	else:
		quantity_names.append(amount.replace('.', ' . '))
	tense_amount = inflection_engine.number_to_words(number_amount)
	tense_amount_2 = tense_amount.replace(',', '')
	quantity_names.append(tense_amount)
	quantity_names.append(tense_amount_2)
	comma_amount = '{:,}'.format(number_amount)
	if(comma_amount != amount.replace('-', '').replace('+', '')):
		quantity_names.append(comma_amount)
		quantity_names.append(comma_amount.replace(',', ' , '))
	return quantity_names

def find_quantity_relation_match(relation_value, paragraph, entities_names, inflection_engine):
	unit = relation_value['unit']
	amount = relation_value['amount']
	unit_names = None
	if('Q' in unit):
		if(unit in entities_names):
			unit_names = entities_names[unit]
	possible_quantity_names = generate_quantity_names(amount, inflection_engine)
	for name in possible_quantity_names:
		pos = paragraph.find(' ' + name + ' ')
		if(pos != -1):
			to_store_amount = amount.replace('-', '- ').replace('+', '+ ')
			if(unit_names is not None):
				to_store_amount ="{} {}".format(amount.replace('-', '- ').replace('+', '+ '), unit_names[0])
			return to_store_amount, pos
	return None, None

def find_globecoordinate_relation_match(relation_value, paragraph):
	latitude = relation_value['latitude']
	longitude = relation_value['longitude']
	globecoordinate_names = ["latitude {} longitude {}".format(latitude, longitude), "longitude {} latitude {}".format(longitude, latitude)]
	hms_str_lat = to_str_deg_min_sec(float(latitude))
	hms_str_lon = to_str_deg_min_sec(float(longitude))
	globecoordinate_names.append(hms_str_lat + ' ' + hms_str_lat)
	globecoordinate_names.append(hms_str_lat + 'N ' + hms_str_lat + 'E')
	generic_form = hms_str_lat.replace('\'', '′').replace('"', '″').replace(" ", "") + 'N ' + hms_str_lat.replace('\'', '′').replace('"', '″').replace(" ", "") + 'E'
	globecoordinate_names.append(generic_form)
	for name in globecoordinate_names:
		pos = paragraph.find(' ' + name + ' ')
		if(pos != -1):
			return generic_form, pos
	return None, None

def load_relations(relations_file):
	relations_dict = {}
	with open(relations_file, 'r') as file:
		for line in file:
			fields = line.split(',')
			relation_name = ""
			for word in fields[2].split(' '):
				relation_name = relation_name + word.capitalize()
			relations_dict[fields[1]] = relation_name
	return relations_dict

def compare_wiki_pedia_x_data(title, entities_names, entities_relations, wiki_pages_dict, relations_dict, inflection_engine):
	wikipedia_sections = wiki_pages_dict[title][2] # all paragraphs
	wikipedia_section_titles = wiki_pages_dict[title][1]
	#print(wikipedia_section_titles)
	wikipedia_paragraphs, wikipedia_sections = process_wiki_sections(wikipedia_section_titles, wikipedia_sections)
	wikidata_relations = entities_relations[title]['relations']
	wikidata_main_entity_id = entities_relations[title]['id']
	pairs = []
	for i in range(len(wikipedia_paragraphs)): #paragraph in wikipedia_paragraphs:
		paragraph = wikipedia_paragraphs[i]
		#section_title = wikipedia_sections[i]
		#print('----------------------')
		#lemmatized_paragraph = lemmatize_paragraph(lemmatizer, paragraph)
		dates = [(date, init_pos) for date, (init_pos, end_pos) in datefinder.find_dates(paragraph, index=True)]
		#print(dates)
		#print("Paragraph: {}".format(paragraph))
		#print("Lemmatized paragraph: {}".format(lemmatized_paragraph))
		#print("Dates: {}".format(dates))
		matches = []
		for relation in wikidata_relations:
			for related_field in wikidata_relations[relation]:
				relation_value = related_field['value']
				relation_type = related_field['type']
				relation_output = None
				relation_pos_on_paragraph = None
				if(relation_type == 'wikibase-entityid'):
					relation_output, relation_pos_on_paragraph = find_entity_relation_match(relation_value, entities_names, paragraph)
				elif(relation_type == 'time'):
					relation_output, relation_pos_on_paragraph = find_time_relation_match(relation_value, paragraph, dates)
				elif(relation_type == 'quantity'):
					relation_output, relation_pos_on_paragraph = find_quantity_relation_match(relation_value, paragraph, entities_names, inflection_engine)
				elif(relation_type == 'globecoordinate'):
					relation_output, relation_pos_on_paragraph = find_globecoordinate_relation_match(relation_value, paragraph)
				if(relation_output is not None):
					# search qualifiers
					relation_qualifiers = related_field['qualifiers']
					found_qualifiers = False
					for qualifier_relation_name in relation_qualifiers:
						qualifier_relation_values = relation_qualifiers[qualifier_relation_name]
						for qualifier_relation_datavalue in qualifier_relation_values:
							qualifier_relation_value = qualifier_relation_datavalue['value']
							qualifier_relation_type = qualifier_relation_datavalue['type']
							qualifier_relation_output = None
							qualifier_relation_pos_on_paragraph = None
							if(qualifier_relation_type == 'wikibase-entityid'):
								qualifier_relation_output, qualifier_relation_pos_on_paragraph = find_entity_relation_match(qualifier_relation_value, entities_names, paragraph)
							elif(qualifier_relation_type == 'time'):
								qualifier_relation_output, qualifier_relation_pos_on_paragraph = find_time_relation_match(qualifier_relation_value, paragraph, dates)
							elif(qualifier_relation_type == 'quantity'):
								qualifier_relation_output, qualifier_relation_pos_on_paragraph = find_quantity_relation_match(qualifier_relation_value, paragraph, entities_names, inflection_engine)
							elif(qualifier_relation_type == 'globecoordinate'):
								qualifier_relation_output, qualifier_relation_pos_on_paragraph = find_globecoordinate_relation_match(qualifier_relation_value, paragraph)
							if(qualifier_relation_output is not None):
								matches.append(["{} {} {} {}".format(relations_dict[relation], relation_output, relations_dict[qualifier_relation_name], qualifier_relation_output), qualifier_relation_pos_on_paragraph])
								found_qualifiers = True
								#print("{} ({}) --{} ({})--> {} ({})".format(title, wikidata_main_entity_id, relations_dict[relation], relation, relation_output, relation_value))
					#if(found_qualifiers is False):
					if(relation in relations_dict):
						matches.append(["{} {}".format(relations_dict[relation].upper(), relation_output), relation_pos_on_paragraph])
		if(len(matches) > 0):
			matches.sort(key=lambda x: x[1])
			output_st = "[INI-SL] "
			for i in range(len(matches)):
				output_st = output_st + matches[i][0]
				if(i < len(matches) - 1):
					output_st = output_st + ' '
				else:
					output_st = output_st + ' [END-SL]'
			input_nl = '[INI-NL] ' + paragraph + ' [END-NL]'
			pairs.append([title, input_nl, output_st])
			#print(matches)
	return pairs

def compare_web_x_data(title, entities_names, entities_relations, web_pages_dict, relations_dict, inflection_engine):
	web_paragraphs = process_web_paragraphs(web_pages_dict[title]) # all paragraphs
	#print(wikipedia_section_titles)
	wikidata_relations = entities_relations[title]['relations']
	wikidata_main_entity_id = entities_relations[title]['id']
	pairs = []
	for paragraph in web_paragraphs:
		#section_title = wikipedia_sections[i]
		#print('----------------------')
		#lemmatized_paragraph = lemmatize_paragraph(lemmatizer, paragraph)
		dates = [(date, init_pos) for date, (init_pos, end_pos) in datefinder.find_dates(paragraph, index=True)]
		#print(dates)
		#print("Paragraph: {}".format(paragraph))
		#print("Lemmatized paragraph: {}".format(lemmatized_paragraph))
		#print("Dates: {}".format(dates))
		matches = []
		for relation in wikidata_relations:
			for related_field in wikidata_relations[relation]:
				relation_value = related_field['value']
				relation_type = related_field['type']
				relation_output = None
				relation_pos_on_paragraph = None
				if(relation_type == 'wikibase-entityid'):
					relation_output, relation_pos_on_paragraph = find_entity_relation_match(relation_value, entities_names, paragraph)
				elif(relation_type == 'time'):
					relation_output, relation_pos_on_paragraph = find_time_relation_match(relation_value, paragraph, dates)
				elif(relation_type == 'quantity'):
					relation_output, relation_pos_on_paragraph = find_quantity_relation_match(relation_value, paragraph, entities_names, inflection_engine)
				elif(relation_type == 'globecoordinate'):
					relation_output, relation_pos_on_paragraph = find_globecoordinate_relation_match(relation_value, paragraph)
				if(relation_output is not None):
					# search qualifiers
					relation_qualifiers = related_field['qualifiers']
					found_qualifiers = False
					for qualifier_relation_name in relation_qualifiers:
						qualifier_relation_values = relation_qualifiers[qualifier_relation_name]
						for qualifier_relation_datavalue in qualifier_relation_values:
							qualifier_relation_value = qualifier_relation_datavalue['value']
							qualifier_relation_type = qualifier_relation_datavalue['type']
							qualifier_relation_output = None
							qualifier_relation_pos_on_paragraph = None
							if(qualifier_relation_type == 'wikibase-entityid'):
								qualifier_relation_output, qualifier_relation_pos_on_paragraph = find_entity_relation_match(qualifier_relation_value, entities_names, paragraph)
							elif(qualifier_relation_type == 'time'):
								qualifier_relation_output, qualifier_relation_pos_on_paragraph = find_time_relation_match(qualifier_relation_value, paragraph, dates)
							elif(qualifier_relation_type == 'quantity'):
								qualifier_relation_output, qualifier_relation_pos_on_paragraph = find_quantity_relation_match(qualifier_relation_value, paragraph, entities_names, inflection_engine)
							elif(qualifier_relation_type == 'globecoordinate'):
								qualifier_relation_output, qualifier_relation_pos_on_paragraph = find_globecoordinate_relation_match(qualifier_relation_value, paragraph)
							if(qualifier_relation_output is not None):
								matches.append(["{} {} {} {}".format(relations_dict[relation], relation_output, relations_dict[qualifier_relation_name], qualifier_relation_output), qualifier_relation_pos_on_paragraph])
								found_qualifiers = True
								#print("{} ({}) --{} ({})--> {} ({})".format(title, wikidata_main_entity_id, relations_dict[relation], relation, relation_output, relation_value))
					#if(found_qualifiers is False):
					if(relation in relations_dict):
						matches.append(["{} {}".format(relations_dict[relation].upper(), relation_output), relation_pos_on_paragraph])
		if(len(matches) > 0):
			matches.sort(key=lambda x: x[1])
			output_st = "[INI-SL] "
			for i in range(len(matches)):
				output_st = output_st + matches[i][0]
				if(i < len(matches) - 1):
					output_st = output_st + ' '
				else:
					output_st = output_st + ' [END-SL]'
			input_nl = '[INI-NL] ' + paragraph + ' [END-NL]'
			pairs.append([title, input_nl, output_st])
			#print(matches)
	return pairs

def store(out_path, shard_id, title, input_nl, output_sl):
	with open("{}inputs.txt-{:05d}-of-01000".format(out_path, shard_id), "a+") as in_file:
		with open("{}outputs.txt-{:05d}-of-01000".format(out_path, shard_id), "a+") as out_file:
			with open("{}titles.txt-{:05d}-of-01000".format(out_path, shard_id), "a+") as title_file:
				in_file.write(input_nl + '\n')
				out_file.write(output_sl + '\n')
				title_file.write(title + '\n')

def main(args):
	wikipedia_txt_file_path = '{}outputs.txt-{:05d}-of-01000'.format(args.wiki_txt_path, args.shard_id)
	web_txt_file_path = '{}inputs.txt-{:05d}-of-01000'.format(args.wiki_txt_path, args.shard_id)
	#titles, wiki_pages_dict = load_wiki_txts(wikipedia_txt_file_path)
	titles, wiki_pages_dict, web_pages_dict = load_wiki_web_txts(wikipedia_txt_file_path, web_txt_file_path)
	#lemmatizer = WordNetLemmatizer()
	p = inflect.engine()
	relations_dict = load_relations('wikidata_relations.csv')
	for batch in chunks(titles, 10):
		#print(batch)
		entities_names, entities_relations = get_titles_aliases_relations_related_entities(batch)
		for title in batch:
			if(title in entities_relations):
				pairs = compare_wiki_pedia_x_data(title, entities_names, entities_relations, wiki_pages_dict, relations_dict, p)
				for data in pairs:
					print("TITLE: {}, {} -> {}".format(data[0], data[1], data[2]))
					store(args.dataset_wiki_out_path, args.shard_id, data[0], data[1], data[2])
				pairs = compare_web_x_data(title, entities_names, entities_relations, web_pages_dict, relations_dict, p)
				for data in pairs:
					print("TITLE: {}, {} -> {}".format(data[0], data[1], data[2]))
					store(args.dataset_web_out_path, args.shard_id, data[0], data[1], data[2])



if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Generate Wikisum dataset from given URLs and Wikipedia descriptions.')
	parser.add_argument('--shard_id', help='shard id (0 to 1000)', default=0, type=int) # 0 ate 1000
	parser.add_argument('--wiki_txt_path', default='wikisum-en/txt/', type=str)
	parser.add_argument('--dataset_wiki_out_path', default='wikisum-en/nl2sl/wiki-only_v3/', type=str)
	parser.add_argument('--dataset_web_out_path', default='wikisum-en/nl2sl/web-only_v3/', type=str)
	args = parser.parse_args()
	main(args)
