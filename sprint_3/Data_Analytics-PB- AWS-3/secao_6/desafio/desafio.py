def extract(path):
    file = open(path, 'r')
    actors = []
    next(file)

    for line in file:
        infoActor = []
        if(line[0] == "\""):

            infoActorError = line.split(',')
            infoActor.append((infoActorError[0] + infoActorError[1]).strip("\""))
            infoActor.append((infoActorError[2]).strip())
            infoActor.append((infoActorError[3]).strip())
            infoActor.append((infoActorError[4]).strip())
            infoActor.append((infoActorError[5]).strip())
            infoActor.append((infoActorError[6]).strip())
            
        else:

            infoActor = line.split(',')

        actors.append({
            'Actor': infoActor[0].strip(),
            'Total Gross': infoActor[1].strip(),
            'Number of Movies': infoActor[2].strip(),
            'Average per Movie': infoActor[3].strip(),
            '#1 Movie': infoActor[4].strip(),
            'Gross': infoActor[5].strip(),
            
        })

    file.close()
    return actors


def transform(list): # Transformação

    for actor in list:
        actor['Total Gross'] = float(actor['Total Gross'])
        actor['Number of Movies'] = int(actor['Number of Movies'])
        actor['Average per Movie'] = float(actor['Average per Movie'])
        actor['Gross'] = float(actor['Gross'])

    return list


def main(): # Principal

    listActorsString = extract('./actors.csv')
    listActors = transform(listActorsString)


    print(f'\nexercise_1:\n') # Exercicio 1

    exercise1 = max(listActors, key=lambda x: x['Number of Movies'])
    print(exercise1['Actor'], exercise1['Number of Movies'], '\n')


    print('*' * 20)
    print(f'\nexercise_2\n') # Exercicio 2

    for actor in listActors:
        print(actor['Actor'], actor['Average per Movie'])

    print('\n')
    print('*' * 30)
    # print('\n')
    print(f'\nexercise_3\n') # Exercicio 3

    exercise3 = max(listActors, key=lambda x: x['Average per Movie'])
    print(exercise3['Actor'],'\n')

    print('*' * 30)
    print(f'\nexercise_4\n') # Exercicio 4

    listMoviesFrequency = {}
    for actor in listActors:
        if actor['#1 Movie'] not in listMoviesFrequency:
            listMoviesFrequency[actor['#1 Movie']] = 1
        else:
            listMoviesFrequency[actor['#1 Movie']] += 1

    movieFrequency = max(listMoviesFrequency, key=listMoviesFrequency.get)
    print(movieFrequency, listMoviesFrequency[movieFrequency])

    print('\n')
    print('*' * 30)
    print(f'\nexercise_5\n') # Exercicio 5

    exercise5 = sorted(listActors, key=lambda d:d['Total Gross'], reverse=True)
    for actor in exercise5:
        print(actor['Actor'], actor['Total Gross'])


if __name__ == '__main__':

    main()




