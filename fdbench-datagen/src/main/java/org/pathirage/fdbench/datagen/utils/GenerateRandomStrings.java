/**
 * Copyright 2016 Milinda Pathirage
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pathirage.fdbench.datagen.utils;

import java.util.Arrays;
import java.util.Random;

public class GenerateRandomStrings {
  private static final String PREFIX =  "http://example.com/";
  private static final String CHARACTERS = "abcdefghijklmnopqrstuvwxyz0123456789/";

  private static final Random nameLengthRand = new Random(System.currentTimeMillis());
  private static  final Random nameRand = new Random(System.currentTimeMillis());

  public static void main(String[] args) {
    for (int i = 0; i < 1000; i++) {
      int nameLength = 6 + nameLengthRand.nextInt(10);
      char[] name = new char[nameLength];
      for (int j = 0; j < nameLength; j++) {
        name[j] = CHARACTERS.charAt(nameRand.nextInt(CHARACTERS.length()));
      }
      String gs = new String(name);
      if(gs.startsWith("/")) {
         gs = gs.substring(1);
      }

      if(gs.endsWith("/")) {
        gs = gs.substring(0, gs.length() - 1);
      }

      System.out.println(PREFIX + gs);
    }
  }
}
