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

package org.pathirage.fdbench.datagen.webperf;

import org.pathirage.fdbench.datagen.api.DataGenerator;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * This is a data generator that simulates a web application request logs of a SOA based application
 * involving multiple front-end servers and multiple back-end services that form a call-tree
 * to serve requests from front-end servers. Front-end servers calls back-end services to serve requests
 * from users for different pages in the website. This generator tries to simulate environment discussed
 * in <a href="http://goo.gl/0jU1ko">http://goo.gl/0jU1ko</a>.
 * <p>
 * The general model used by this generator is the following. There is a set of web pages served by a web application
 * and there are multiple users accessing these web pages in a given time. For each page access front-end service calls
 * one or more back-end services to get the content required to build the page. These back-end services may also call
 * other back-end services forming tree like structure of call shown below:
 * <p>
 * <pre>{@code
 *                                +-> svc_1 ---> svc_3
 *                               /          +---> svc_10
 *                             /           /
 *   http://example.com/page1 +-----> svc_2 ----> svc_4
 *                             \
 *                              \
 *                              +-> svc_5
 * }
 * </pre>
 * <p>
 * <p>
 * Each page has a page_id and each page access has a unique id associated with it. Front-end server will send this
 * unique id to downstream services and those services will also send the id to their downstream services.
 * Each front-end and back-end service call will generate a event that get published into a message bus for monitoring
 * purposes.
 * <p>
 * <p>
 * Multiple instances of this generator will run in parallel.
 */
public class WebCallTreeEventGenerator implements DataGenerator {
  private static final String WEB_PAGES_FILE = "web_pages.txt";
  private static final String SVC_NAMES_FILE = "service_names.txt";
  private static final int MIN_SVCS = 4;

  private final Mode mode;
  private final int maxCallTreeDepth;
  private final int maxServicesInOneLevel;
  private final int numberOfFrontEndServers;
  private final int numberOfWebPages;
  private final int maxDeliveryDelayMilliseconds;
  private final Random urlRand;
  private final Random svcRand;

  private final Set<String> webPages = new HashSet<>();
  private final Map<Integer, Set<String>> backendServices = new HashMap<>();
  private final Set<String> selectedNames = new HashSet<>();
  private final Map<String, Object> callTreeTemplate = new HashMap<>();

  public WebCallTreeEventGenerator(Mode mode, int maxCallTreeDepth, int maxServicesInOneLevel,
                                   int numberOfFrontEndServers, int numberOfWebPages,
                                   int maxDeliveryDelayMilliseconds, long rnSeed) {
    this.mode = mode;
    this.maxCallTreeDepth = maxCallTreeDepth;
    this.maxServicesInOneLevel = maxServicesInOneLevel;
    this.numberOfFrontEndServers = numberOfFrontEndServers;
    this.numberOfWebPages = numberOfWebPages;
    this.maxDeliveryDelayMilliseconds = maxDeliveryDelayMilliseconds;
    this.urlRand = new Random(rnSeed);
    this.svcRand = new Random(rnSeed);
  }

  private void genWebPages() throws URISyntaxException, IOException {
    List<String> pages =
        Files.readAllLines(Paths.get(this.getClass().getResource(WEB_PAGES_FILE).toURI()), Charset.defaultCharset());
    for (int i = 0; i < numberOfWebPages; i++) {
      webPages.add(pages.get(urlRand.nextInt(pages.size())));
    }
  }

  private void generateServices() throws URISyntaxException, IOException {
    List<String> allSvcNames =
        Files.readAllLines(Paths.get(this.getClass().getResource(SVC_NAMES_FILE).toURI()), Charset.defaultCharset());
    for (int i = 0; i < maxCallTreeDepth; i++) {
      int numSvcs = MIN_SVCS + svcRand.nextInt(maxServicesInOneLevel);
      Set<String> svcNames = new HashSet<>();
      for (int j = 0; j < numSvcs; i++) {
        String svcName = allSvcNames.get(svcRand.nextInt(allSvcNames.size()));
        while (selectedNames.contains(svcName)) {
          svcName = allSvcNames.get(svcRand.nextInt(allSvcNames.size()));
        }

        svcNames.add(svcName);
        selectedNames.add(svcName);
      }

      backendServices.put(i, svcNames);
    }
  }

  private void generateCallTreeTemplates() {
    for (String webPage : webPages) {

    }
  }

  private void init() throws IOException, URISyntaxException {
    genWebPages();
    generateServices();
  }

  @Override
  public void run() {

  }
}
