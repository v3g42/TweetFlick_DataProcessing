/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Trends;

import Trends.DataStructures.DBFreq;
import Trends.DataStructures.DBWord;
import Trends.DataStructures.Freq;
import Trends.DataStructures.ProbableWord;
import Trends.DataStructures.ProbablesList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

/**
 *
 * @author karthik
 */
public class Merger {

    ProbablesList probablesList;

    public Merger(ProbablesList probablesList) {
        this.probablesList = probablesList;
    }

    void startMerging() {
        ArrayList<DBWord> dbwordsArr = DBAdapter.readDB();
        for (DBWord dbword : dbwordsArr) {
            updateDBWord(dbword);
        }
        probablesList.updateProbables();
        PriorityQueue<DBWord> dbwordsHeap = new PriorityQueue<DBWord>();
        dbwordsHeap.addAll(dbwordsArr);
        ProbableWord[] probArr = probablesList.getSortedArray();
        for (int i = probArr.length - 1; i >= 0; i--) {
            ProbableWord cur = probArr[i];
            int comparingScore = cur.totalScore*9;
            DBWord least = dbwordsHeap.peek();
            if(comparingScore > least.totalScore){
                dbwordsHeap.poll();
                LinkedList<DBFreq> newHistory = new LinkedList<DBFreq>();
                newHistory.add(new DBFreq(cur.currentFreq, 1));
                DBWord curDbword = new DBWord(cur.word,comparingScore,newHistory);
                dbwordsHeap.add(curDbword);
            }
        }
        ArrayList<DBWord> toupdate = new ArrayList<DBWord>();
        for(DBWord word: dbwordsHeap){
            toupdate.add(word);
        }
        DBAdapter.updateTrends(toupdate);
    }

    private void updateDBWord(DBWord dbword) {
        //values are hardcoded here..history will be 1,1,1,1,1,1,6,6,6;

        int L2 = 6;
        int HISTORY_MAX = 9;
        int L2start = 6;
        int L2end = 8;
        int L2maxLimit = 6;
        int totalScore = 0;
        Freq freqToUpdate = new Freq(0, 0, 0);
        if (probablesList.contains(dbword.word)) {
            ProbableWord word = probablesList.get(dbword.word);
            freqToUpdate = word.currentFreq;
        }
        LinkedList<DBFreq> history = dbword.history;
        if (history.size() < HISTORY_MAX) {
            while (history.size() != HISTORY_MAX) {
                history.add(new DBFreq(0, 0, 0, 0));
            }
        }
        history.addFirst(new DBFreq(freqToUpdate, 1));
        DBFreq prev = history.get(L2start - 1);
        history.remove(L2start - 1);
        if (prev.limit == 0) {
            return;
        }
        for (int i = L2start; i <= L2end; i++) {
            DBFreq cur = history.get(i);
            DBFreq temp = prev;
            if (cur.limit == L2maxLimit) {
                Freq avg = cur.freq.div(L2maxLimit);
                prev = new DBFreq(avg, L2maxLimit);
                cur.freq.add(temp.freq);
                cur.freq.subtract(avg);
            } else {
                cur.freq.add(temp.freq);
                cur.limit++;
                break;
            }
        }
        for (int i = 0; i < HISTORY_MAX; i++) {
            int mul = 1;
            if (i < 3) {
                mul = 10;
            } else if (i < 6) {
                mul = 8;
            } else if (i == 6) {
                mul = 6;
            } else if (i == 7) {
                mul = 4;
            } else if (i == 8) {
                mul = 2;
            }
            totalScore+=(history.get(i).freq.score*mul);
        }
        dbword.totalScore=totalScore;
    }
}
