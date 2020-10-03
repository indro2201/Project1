import java.util.*;

class Cards {
    int value;
    String suit;

    public void setCards(String suit, int r) {
        this.value = r;
        this.suit = suit;
    }

    //Write your code here.

    int getSuitPriority(String suitString) {
        int priorityValue=0;
        //Write your code here.
        //Spades > Diamond > Hearts > Clubs
        switch(suitString){
            case "spades": priorityValue= 4; break;
            case "diamonds": priorityValue= 3; break;
            case "hearts": priorityValue= 2; break;
            case "clubs": priorityValue= 1; break;
            default: System.out.println("Incorrect Suit"); java.lang.System.exit(-1);// if incorrect suit is entered
        }
        return priorityValue;
    }
}

class ClassicGame {
    int points1 = 0;
    int points2 = 0;

    public void game(Cards c1, Cards c2) {

        //Write your code here.
        if (c1.value==c2.value){
            if (c1.getSuitPriority(c1.suit)>c2.getSuitPriority(c2.suit)){
                points1++;
            }else if (c1.getSuitPriority(c1.suit)<c2.getSuitPriority(c2.suit)) {
                points2++;
            }//assigning no points in case of tie in the suits
        }
        else
        {
            if(c1.value> c2.value){
                points1++;
            }
            else {
                points2++;
            }
        }
        System.out.println("points of player 1 :" + " " + points1);
        System.out.println("points of player 2 :" + " " + points2);
    }

}

public class CardGame {

    public static void main(String[] args) {
        //Two objects (cards) of class Cards
        Cards c1 = new Cards();
        Cards c2 = new Cards();
        Scanner in = new Scanner(System.in);
        //Enter the suit of card 1
        String s1 = in.nextLine();
        c1.setCards(s1, in.nextInt());
        in.nextLine();
        //Enter the suit of card 2
        String s2 = in.nextLine();
        c2.setCards(s2, in.nextInt());
        ClassicGame c = new ClassicGame();
        c.game(c1, c2);
    }
}