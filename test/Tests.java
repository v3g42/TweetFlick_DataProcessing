
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Karthik
 */
public class Tests {
    public static void main(String[] args) {
        testhash();
    }

    static void testhash(){
        HashMap<Integer, String> map = new HashMap<Integer,String>();
        for(int i=0;i<10;i++){
        map.put(i,"a"+i);
        }
        Collection<String> c = map.values();
        Iterator i = c.iterator();
        System.out.println("before:"+Runtime.getRuntime().freeMemory());
        map=null;
        System.gc();
         System.out.println("after:"+Runtime.getRuntime().freeMemory());
        map.clear();
        while(i.hasNext()) System.out.println(""+i.next());
    }
}
