import java.io.*;
import java.lang.reflect.Array;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import java.util.Map.Entry;
import java.util.stream.Collectors;

public class MP0 {
    Random generator;
    String userName;
    String delimiters = " \t,;.?!-:@[](){}_*/";
    String[] stopWordsArray = {"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
            "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its",
            "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that",
            "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having",
            "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
            "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
            "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
            "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
            "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
            "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"};

    public MP0(String userName) {
        this.userName = userName;
    }


    public Integer[] getIndexes() throws NoSuchAlgorithmException {
        Integer n = 10000;
        Integer number_of_lines = 50000;
        Integer[] ret = new Integer[n];
        long longSeed = Long.parseLong(this.userName);
        this.generator = new Random(longSeed);
        for (int i = 0; i < n; i++) {
            ret[i] = generator.nextInt(number_of_lines);
        }
        return ret;
    }

    public String[] process() throws Exception{
    	String[] topItems = new String[20];
        Integer[] indices = getIndexes(); // rename from indexes

        //TO DO
        // Read input from stdin into list of sentences (ArrayList)
        // https://docs.oracle.com/javase/8/docs/api/java/util/Scanner.html
        List<ArrayList<String>> inputSentences = new ArrayList<ArrayList<String>>();
        Scanner sc = new Scanner(System.in);
        String sentence = "";
        while (sc.hasNextLine()) {
            sentence = sc.nextLine();
            /**
             * 1. Divide each sentence into a list of words using delimiters provided 
             * in the “delimiters” variable.
             */
            ArrayList<String> words = new ArrayList<String>();
            StringTokenizer st = new StringTokenizer(sentence, delimiters);
            String token = "";
            while (st.hasMoreTokens()) {
                // System.out.println(st.nextToken());
                /**
                 * 2. Make all the tokens lowercase and remove any tailing and leading spaces.
                 */
                token = st.nextToken().toLowerCase().trim();

                /**
                 * 3. Ignore all common words provided in the “stopWordsArray” variable.
                 */
                if(!Arrays.stream(stopWordsArray).anyMatch(token::equals)) {
                    words.add(token);
                }
            }

            inputSentences.add(words);
        }
        sc.close();
        // System.out.println(inputSentences.size());
        // for (ArrayList<String> sentences : inputSentences) {
        //     for (String word : sentences) {
        //         System.out.println(word);
        //     }
        // }
        // int max = Collections.max(Arrays.asList(indices)); 
        // System.out.println(max);

        /**
         * 4. Keep track of word frequencies. To make the application more interesting, 
         * you have to process only the titles with certain indexes. These indexes are 
         * accessible using the “getIndexes” method, which returns an Integer Array with
         * 0-based indexes to the input file. It is possible to have an index appear
         * several times. In this case, just process the index multiple times.
         */

        // use map with word as key and count as value
        Map<String, Integer> frequencyCounter = new HashMap<>();
        // loop through the indices
        for (int i : indices) {
            ArrayList<String> words = inputSentences.get(i);
            for (String w : words) {
                Integer n = frequencyCounter.get(w);
                n = (n == null) ? 1 : ++n;
                frequencyCounter.put(w, n);
            }
        }

        /** 
         * 5. Sort the words by frequency in a descending order. If two words have the 
         * same number count, use the lexigraphy.
         */
        // https://www.javacodegeeks.com/2017/09/java-8-sorting-hashmap-values-ascending-descending-order.html
        Map<String, Integer> sortedMap = frequencyCounter
            .entrySet()
            .stream()
            .sorted(Collections.reverseOrder(Entry.comparingByValue()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                           (o1, o2) -> o1, LinkedHashMap::new));
        
        /**
         * 6. Print out the top 20 items from the sorted list as a String Array.
         */
        Set<String> keys = sortedMap.keySet();
        String[] keysArray = keys.toArray(new String[keys.size()]);
        for(int i=0; i<keysArray.length && i<20; i++) {
            // System.out.println(keysArray[i]);
            topItems[i] = keysArray[i];
        }
                                    
		return topItems;
    }

    public static void main(String args[]) throws Exception {
    	if (args.length < 1){
    		System.out.println("missing the argument");
    	}
    	else{
            // used as a seed for random number generator to create indices
            String userName = args[0]; 
            
	    	MP0 mp = new MP0(userName);
	    	String[] topItems = mp.process();

	        for (String item: topItems){
	            System.out.println(item);
	        }
	    }
	}

}
