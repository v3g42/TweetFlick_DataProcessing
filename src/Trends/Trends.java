/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package Trends;

/**
 *
 * @author karthik
 */
public class Trends {
    static long lastMergeTime;
    static final long MERGE_INTERVAL=60*60*1000;

    static void addWord(String word,boolean isCeleb){
        //addToProbables(word);
        if(System.currentTimeMillis()-lastMergeTime > MERGE_INTERVAL){
//            merge();
        }
    }

}
