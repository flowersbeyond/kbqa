from sparql_check.dbp_check import QALD_4_Checker as check4, QALD_5_Checker as check5, QALD_6Plus_Checker as check6plus

if __name__ == '__main__':
    check4.main(False)
    check5.main(False)
    check6plus.main(False)

