import requests 
import json 
import random
import html

url = "https://opentdb.com/api.php?amount=1&category=12&difficulty=easy&type=multiple"

endGame = "" 

score_correct = 0 
score_incorrect = 0

while endGame != "quit": 
    r = requests.get(url)
    if r.status_code != 200: 
        endGame=input("Sorry there was an error retriving the question press enter to try again or press quit to quit")
    else: 
        valid_answer = False
        answer_number = 1 
        data = json.loads(r.text)
        question = data['results'][0]['question']
        answer = data['results'][0]['incorrect_answers']
        correct_answer = data['results'][0]['correct_answer']
        answer.append(correct_answer)
        random.shuffle(answer)

        print(html.unescape(question) + "\n")

        for i, a in enumerate(answer): 
            print(str(answer_number) + " - " + html.unescape(a))
            answer_number +=1

        while valid_answer == False: 
            user_answer = input("\nType the number of the correct answer: ")

            try: 
                user_answer = int(user_answer)
                if user_answer > len(answer) or user_answer <= 0: 
                    print("invalid answer")
                else: 
                    valid_answer = True
            
            except: 
                print("invalid answer only numbers")

        user_answer = answer[int(user_answer)-1]

        if user_answer == correct_answer: 
            print(f'\nCongrats you are correct {html.unescape(correct_answer)} ')

            score_correct += 1

        else: 
            print(f'\nSorry {html.unescape(user_answer)} is incorrect the correct answer is {html.unescape(correct_answer)}. ')
            score_incorrect += 1 

        print('\n#######################################')
        print(f'Your score is: {round(((score_correct / (score_correct + score_incorrect)) * 100), 2)}%')
        print(f'Correct answer: {str(score_correct)}')
        print(f'Incorrect anser: {str(score_incorrect)}')
        print('\n#######################################')
      

        endGame = input("\nPress enter to play or type quit to quit the Game: ").lower()

print('\nThanks For Playing')