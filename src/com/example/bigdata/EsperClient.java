package com.example.bigdata;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import net.datafaker.Faker;

import net.datafaker.transformations.JsonTransformer;
import net.datafaker.transformations.Schema;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static net.datafaker.transformations.Field.field;
import java.util.Random;

public class EsperClient {
    public static void main(String[] args) throws InterruptedException {
        int noOfRecordsPerSec;
        int howLongInSec;
        if (args.length < 2) {
            noOfRecordsPerSec = 100;
            howLongInSec = 70;
        } else {
            noOfRecordsPerSec = Integer.parseInt(args[0]);
            howLongInSec = Integer.parseInt(args[1]);
        }

        Configuration config = new Configuration();
        EPCompiled epCompiled = getEPCompiled(config);

        // Connect to the EPRuntime server and deploy the statement
        EPRuntime runtime = EPRuntimeProvider.getRuntime("http://localhost:port", config);
        EPDeployment deployment;
        try {
            deployment = runtime.getDeploymentService().deploy(epCompiled);
        }
        catch (EPDeployException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        EPStatement resultStatement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "answer");

        // Add a listener to the statement to handle incoming events
        resultStatement.addListener( (newData, oldData, stmt, runTime) -> {
            for (EventBean eventBean : newData) {
                System.out.printf("R: %s%n", eventBean.getUnderlying());
            }
        });

        Faker faker = new Faker();
        String record;
        String[] subscriptionIds= new String[10];
        for (int i = 0; i < subscriptionIds.length; i++) {
            subscriptionIds[i] = String.format("subID-%d",i);//faker.azure().subscriptionId(); // dodajemy elementy do tablicy
        }

        //SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy", Locale.ENGLISH);
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() < startTime + (1000L * howLongInSec)) {
            for (int i = 0; i < noOfRecordsPerSec; i++) {

                    Random random = new Random();
                    int randomNumber = random.nextInt(3) + 1;
                    String randomString = String.valueOf(randomNumber);
//                    String resourceGroup = faker.azure().resourceGroup();
                    String resourceGroup = "rg-" + randomString;
                    String subscriptionId = subscriptionIds[faker.number().randomDigit()%10];
                    int randomNumber2 = random.nextInt(4) + 1;

                    String reg = "default";
                    switch (randomNumber2) {
                        case 1: reg = "germany"; break;
                        case 2: reg = "uk"; break;
                        case 3: reg = "asia"; break;
                        case 4: reg = "india"; break;
                        case 5: reg = "ukraine"; break;
                    }
//                    String region = faker.azure().region();
                    String region = reg;
                    //zmiana rozkładu wartości — rzadsze wartości wysokie.
                    double cost = Math.round(Math.pow(faker.number().randomDouble(2, 0, 10),3)*100.0)/100.0;
                    Timestamp eTimestamp = faker.date().past(30, TimeUnit.SECONDS);
                    eTimestamp.setNanos(0);
                    Timestamp iTimestamp = Timestamp.valueOf(LocalDateTime.now().withNano(0));

                Schema<Object, ?> schema = Schema.of(
                        field("resourceGroup", () -> resourceGroup),
                        field("subscriptionId", () -> subscriptionId),
                        field("region", () -> region),
                        field("cost", () -> cost),
                        field("ets", eTimestamp::toString),
                        field("its", iTimestamp::toString)
                );
//                System.out.println(resourceGroup);
                System.out.println(region + " " + cost + " " + iTimestamp + " " + eTimestamp + " " + resourceGroup);
//                System.out.println(region);
//                System.out.println(cost);
//                System.out.println(eTimestamp);
//                System.out.println(iTimestamp);

                JsonTransformer<Object> transformer = JsonTransformer.builder().build();
                record = transformer.generate(schema, 1);
                runtime.getEventService().sendEventJson(record, "AzureCostEvent");
            }
            waitToEpoch();
        }
    }

    private static EPCompiled getEPCompiled(Configuration config) {
        CompilerArguments compilerArgs = new CompilerArguments(config);

        // Compile the EPL statement
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        EPCompiled epCompiled;
        try {
            epCompiled = compiler.compile("""
                    @public @buseventtype create json schema AzureCostEvent(resourceGroup string, subscriptionId string, region string, cost double, ets string, its string);
                    
//                    ZAD1
//                    @name('answer')
//                    SELECT subscriptionId, sum(cost) as suma
//                    FROM AzureCostEvent#ext_timed(java.sql.Timestamp.valueOf(its).getTime(), 10 sec)
//                    GROUP BY subscriptionId



                      ZAD2
//                    @name('answer')
//                    SELECT ets, cost, resourceGroup, subscriptionId, region
//                    FROM AzureCostEvent#length(1)
//                    WHERE cost >= 800



//                    ZAD3
//                    @name('answer')
//                    SELECT ets, cost, subscriptionId, sum(cost) as suma
//                    FROM AzureCostEvent#ext_timed(java.sql.Timestamp.valueOf(its).getTime(), 5 sec)
//                    GROUP BY subscriptionId
//                    HAVING (sum(cost) / (sum(cost) - cost)) > 2 AND (sum(cost) != cost)
                    
                    
                    
//                    ZAD4
//                    create window ThirtySecondWindow#ext_timed(java.sql.Timestamp.valueOf(its).getTime(), 30)(region string, resourceGroup string, numberOfExceeds int, its string);
                   
//                    insert into ThirtySecondWindow
//                    select
//                        region,
//                        resourceGroup,
//                        1 as numberOfExceeds,
//                        max(its) as its
//                    from AzureCostEvent#ext_timed_batch(java.sql.Timestamp.valueOf(its).getTime(), 10)
//                    group by region, resourceGroup
//                    having sum(cost) > 500;
//                   
//                    @name('answer')
//                    select region, sum(numberOfExceeds) as how_many from ThirtySecondWindow
//                    group by region
//                    having sum(numberOfExceeds) >=2 and count(distinct resourceGroup) >= 2
//                    output snapshot every 10 seconds
                    
                    
                    
//                    ZAD5
//                    create window ThirtySecondWindow#ext_timed(java.sql.Timestamp.valueOf(its).getTime(), 30) as AzureCostEvent;
//                    insert into ThirtySecondWindow
//                    select * from AzureCostEvent;
//                   
//                   
//                    @name('answer')
//                    select a[0].ets as start_ets,
//                    a[0].cost as cost1,
//                    a[1].cost as cost2,
//                    b.ets as penalty_ets,
//                    b.cost as penalty_cost,
//                    b.region as region
//                   
//                    from pattern[
//                    every([2:] a=ThirtySecondWindow(cost > 200 and cost <= 500) until b=ThirtySecondWindow(cost > 500))
//                    ]
//                    where a.distinctOf(i => region).countOf()  = 1 and a[0].region = b.region



//                    ZAD6
//                    @name('answer')
//                    select
//                        uk.resourceGroup as resourceGroup,
//                        uk.ets as ets1,
//                        germany.ets as ets2,
//                        asia.ets as ets3,
//                        uk.cost + germany.cost + asia.cost as cost
//                    from pattern [every
//                        (
//                        uk=AzureCostEvent(region='uk' and cost > 200)
//                        ->
//                        (
//                        not AzureCostEvent((cost < 10 and resourceGroup = uk.resourceGroup) or (region in ('uk','asia','india', 'germany') and resourceGroup = uk.resourceGroup))
//                        until
//                        germany=AzureCostEvent(region='germany' and cost > 200 and resourceGroup = uk.resourceGroup)
//                        )
//                        ->
//                        (
//                        not AzureCostEvent((cost < 10 and resourceGroup = germany.resourceGroup) or (region in ('uk','germany', 'asia', 'india') and resourceGroup=germany.resourceGroup))
//                        until
//                        asia=AzureCostEvent(region in ('asia', 'india') and cost > 200 and resourceGroup = germany.resourceGroup)
//                        )
//                        ) ]



//                    ZAD7
//                    @name('answer')
//                    SELECT *
//                    FROM AzureCostEvent
//                    MATCH_RECOGNIZE(
//                    PARTITION BY region, resourceGroup
//                    MEASURES
//                      STRT.region as region,
//                      STRT.resourceGroup as resourceGroup,
//                      STRT.ets as ets,
//                      STRT.cost as discount,
//                      sum(UP.cost) + STRT.cost as beforeDiscount,
//                      sum(UP.cost) as afterDiscount,
//                      count(UP.cost) + 1 as length
//                    PATTERN(STRT UP{2,} DOWN)
//                    DEFINE
//                      UP AS UP.cost > PREV(UP.cost),
//                      DOWN AS DOWN.cost <= PREV(DOWN.cost))
                    """, compilerArgs);
        }
        catch (EPCompileException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }
        return epCompiled;
    }

    static void waitToEpoch() throws InterruptedException {
        long millis = System.currentTimeMillis();
        Instant instant = Instant.ofEpochMilli(millis) ;
        Instant instantTrunc = instant.truncatedTo( ChronoUnit.SECONDS ) ;
        long millis2 = instantTrunc.toEpochMilli() ;
        TimeUnit.MILLISECONDS.sleep(millis2+1000-millis);
    }
}

