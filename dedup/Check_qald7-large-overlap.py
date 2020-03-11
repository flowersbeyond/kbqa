import json
import csv

def load_qald7_test(data_file):
    data = {}
    with open(data_file, encoding='utf-8') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=';')

        for row in spamreader:
            data[row[1].strip().lower()] = row[0]

    return data

def extract_qald_multilingual_english_questions(file):
    questions = {}
    with open(file, encoding='utf-8') as fin:
        for l in fin:
            question = json.loads(l)
            q_strings = question['question']
            for q in q_strings:
                if q['language'] == 'en':
                    questions[question['id']] = q['string'].strip().lower()
                    break


    return questions

if __name__ == '__main__':
    data_dir = './data/QALD/7/data/large-scale/'

    qald7_test_questions = {}
    for i in range(1, 4):
        qald7_test_questions.update(load_qald7_test(data_dir + '/qald-7-test-largescale_part%d.csv' % i))


    qald_multilingual_train = './data/QALD/train-multilingual-4-9.jsonl'
    questions = extract_qald_multilingual_english_questions(qald_multilingual_train)
    print('train dup questions:')
    qald7_temp_set = set()
    for qid in questions:
        if questions[qid] in qald7_test_questions:
            qald7_temp_set.add(qald7_test_questions[questions[qid]])
            print('%s\t%s' % (qald7_test_questions[questions[qid]], questions[qid]))
    #for i in range(0, 150):
    #    if str(i) not in qald7_temp_set:
    #        print (i)


    qald_multilingual_test = './data/QALD/test-multilingual-4-9.jsonl'
    questions = extract_qald_multilingual_english_questions(qald_multilingual_test)
    print('test dup questions:')
    qald7_temp_set = set()
    for qid in questions:
        if questions[qid] in qald7_test_questions:
            qald7_temp_set.add(qald7_test_questions[questions[qid]])
            print('%s\t%s' % (qald7_test_questions[questions[qid]], questions[qid]))
    #for i in range(0, 150):
    #    if str(i) not in qald7_temp_set:
    #        print(i)