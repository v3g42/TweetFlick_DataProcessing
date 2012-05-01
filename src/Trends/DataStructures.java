/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Trends;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;

/**
 *
 * @author Karthik
 */
public class DataStructures {

    public static class ProbablesList {
        //a ds which supports search by word in O(1) time (hashmap<String word, ProbableWord>) and
        //sort by totalScore in O(logn) time (priority queue <ProbableWord> ,compare with totalScore)

        private HashMap<String, ProbableWord> map = new HashMap<String, ProbableWord>();
        private PriorityQueue<ProbableWord> heap = new PriorityQueue<ProbableWord>();
        private int maxSize;
        private int currentSize;

        public ProbablesList(int size) {
            maxSize = size;
            currentSize = 0;
        }

        public void put(ProbableWord word) {
            if (currentSize >= maxSize) {
                ProbableWord least = heap.peek();
                if (least.totalScore < word.totalScore) {
                    //can do remove but want to do poll for efficiency
                    heap.poll();
                    map.remove(least.word);
                    map.put(word.word, word);
                    heap.add(word);
                } else {
                    return;
                }
            } else {
                map.put(word.word, word);
                heap.add(word);
                currentSize++;
            }

        }

        public ProbableWord get(String word) {
            return map.get(word);
        }

        public void remove(ProbableWord word) {
            if (currentSize == 0) {
                return;
            }
            map.remove(word.word);
            heap.remove(word);
            currentSize--;
        }

        public int size() {
            return currentSize;
        }

        public void clear() {
            map.clear();
            heap.clear();
            currentSize = 0;
        }

        public boolean contains(String word) {
            return map.containsKey(word);
        }

        public void updateProbables(){
            Collection<ProbableWord> coll = map.values();
            Iterator<ProbableWord> iter = coll.iterator();
            while(iter.hasNext()){
                ProbableWord word = iter.next();
                Freq lastFreq = word.oldFreqs.getLast();
//                word.totalScore-= lastFreq.cc*T
                word.oldFreqs.removeLast();
                word.oldFreqs.addFirst(word.currentFreq);
                word.currentFreq = new Freq(0,0,0);
            }

        }

        public ProbableWord[] getSortedArray(){
            Collection<ProbableWord> coll = map.values();
            ProbableWord[] probArr = new ProbableWord[currentSize];
            int i=0;
            for(ProbableWord word:coll){
                probArr[i++] = word;
            }
            Arrays.sort(probArr);
            return probArr;
        }
    }

    public static class ProbableWord implements Comparator<Object> {

        String word;
        int totalScore;
        Freq currentFreq;
        LinkedList<Freq> oldFreqs = new LinkedList<Freq>();
        boolean isHashtag = false;

        public ProbableWord(String word) {
           
            if (word.startsWith("#")) {
                isHashtag = true;
                word=word.substring(1);
            }
             this.word = word;
            this.totalScore = 0;
            currentFreq = new Freq(0,0,0);
        }

        public void increment(boolean isCeleb) {
            if (isCeleb) {
                currentFreq.cc++;

            } else {
                currentFreq.fc++;
            }
            int addedScore = 1 * getScoreFactor(isCeleb);
            totalScore += addedScore;
        }

        public int getScoreFactor(boolean isCeleb){
             int ret = 1;
            if (isHashtag) {
                ret *= TrendsConstants.HASH_isto_WORD;
            }
            if (isCeleb) {
                ret *= TrendsConstants.CELEB_isto_FAN;
            }
             return ret;
        }

        @Override
        public String toString() {
            return word + ":" + totalScore;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 41 * hash + (this.word != null ? this.word.hashCode() : 0);
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            return this.word.equals(o);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return new Integer(((ProbableWord) o1).totalScore).compareTo(((ProbableWord) o2).totalScore);
        }
    }

    public static class Freq{
       int cc;
       int fc;
       int score;
       public Freq(int c,int f,int s){
           this.cc=c;
           this.fc=f;
           this.score=0;
       }

       public void add(Freq f){
           this.cc+=f.cc;
           this.fc+=f.fc;
           this.score+=f.score;
       }
       public void subtract(Freq f){
            this.cc-=f.cc;
           this.fc-=f.fc;
           this.score-=f.score;
       }
       public Freq div(int l){
           int c = this.cc/=l;
           int f =this.fc/=l;
           int s =this.score/=l;
           return new Freq(c, f, s);
       }
    }

    public static class DBFreq{
        Freq freq;
        int limit;
        public DBFreq(int c,int f,int s,int limit){
            this.freq = new Freq(c,f,s);
            this.limit = limit;
        }
        public DBFreq(Freq freq,int limit){
            this.limit = limit;
            this.freq = freq;
        }
    }
    public static class DBWord implements Comparator<Object> {

        String word;
        int totalScore;
        LinkedList<DBFreq> history;

        public DBWord(String word, int score, LinkedList<DBFreq> history) {
            this.word = word;
            this.totalScore = score;
            this.history = history;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 79 * hash + (this.word != null ? this.word.hashCode() : 0);
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            return this.word.equals(o);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return new Integer(((DBWord) o1).totalScore).compareTo(((DBWord) o2).totalScore);
        }
    }
}
