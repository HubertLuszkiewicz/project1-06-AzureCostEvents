# Charakterystyka danych

Pewna wielka firma monitoruje swoje wydatki związane z korzystaniem platformy chmurowej Azure.

W strumieniu pojawiają się zdarzenia zgodne ze schematem `AzureCostEvent`.

```
create json schema AzureCostEvent(resourceGroup string, subscriptionId string, region string, cost double, ets string, its string);
```

Każde zdarzenie związane z jest z faktem naliczenia określonych kosztów za wykorzystanie grupy zasobów w ramach danej subskrypcji (oddziału firmy) i regionu.  

Dane uzupełnione są o dwie etykiety czasowe. 
* Pierwsza (`ets`) związana jest z momentem naliczenia kosztów. 
  Etykieta ta może się losowo spóźniać w stosunku do czasu systemowego maksymalnie do 30 sekund.
* Druga (`its`) związana jest z momentem rejestracji naliczenia kosztów w systemie.

# Opis atrybutów

Atrybuty w każdym zdarzeniu zgodnym ze schematem `AzureCostEvent` mają następujące znaczenie:

- `resourceGroup` - nazwa grupy zasobów
- `subscriptionId` - nazwa subskrypcji
- `region` - nazwa regionu Azure, w którym zlokalizowane były wykorzystane zasoby
- `cost` - koszt w dolarach
- `ets` - czas naliczenia kosztów
- `its` - czas rejestracji faktu naliczenia kosztów w systemie

# Zadania
Opracuj rozwiązania poniższych zadań. 
* Opieraj się strumieniu zdarzeń zgodnych ze schematem `AzureCostEvent`
* W każdym rozwiązaniu możesz skorzystać z jednego lub kilku poleceń EPL.
* Ostatnie polecenie będące ostatecznym rozwiązaniem zadania musi 
  * być poleceniem `select` 
  * posiadającym etykietę `answer`, przykładowo:
  ```aidl
  	@name('answer')
        select * from  AzureCostEvent#ext_timed(java.sql.Timestamp.valueOf(its).getTime(), 3 sec)
  ```

## Zadanie 1
Dla każdej subskrypcji generuj informacje dotyczące sumy kosztów zarejestrowanych w ciągu ostatnich 10 sekund.

Wyniki powinny zawierać następujące kolumny:
- `subscriptionId` - subskrypcja, w której naliczono koszty
- `suma` - sumaryczna wartość zarejestrowanych kosztów

## Zadanie 2
Wykrywaj przypadki jednorazowych naliczeń bardzo wysokich kosztów (większych lub równych 800$). 

Wyniki powinny zawierać następujące kolumny:
- `ets` - datę naliczenia kosztów
- `cost` - naliczony koszt
- `resourceGroup` - nazwę grupy zasobów
- `subscriptionId` - subskrypcja, w której naliczono koszty
- `region` - region, w ramach którego naliczono koszty

## Zadanie 3

Wykrywaj przypadki, w których pojedyncze naliczenie zwiększa sumaryczny koszt zarejestrowany w ramach tej samej subskrypcji przez ostatnie 5 sekund o ponad 100% (pomiń przypadki, w których w ciągu ostatnich 5 sekund zarejestrowano tylko jedno zdarzenie).

Wyniki powinny zawierać następujące kolumny:
- `ets` - datę naliczenia kosztów
- `cost` - naliczony koszt
- `subscriptionId` - subskrypcja, w której naliczono koszty.
- `suma` - sumaryczna wartość kosztów dla subskrypcji zarejestrowanych w ciągu ostatnich 5 sekund

## Zadanie 4
Koszty zarejestrowane dla każdych kolejnych 10 sekund, są sumowane na poziomie grupy zasobów i regionów. 
Firma wyznaczyła dla każdego z regionów limit na tak wyliczany koszt za pojedynczą grupę zasobów wynoszący 500$.
 
Wykrywaj przypadki, w których w ciągu trzech ostatnich 10-sekundowych przedziałów czasu jeden z regionów przekroczył ustalony limit co najmniej 2 razy dla co najmniej dwóch różnych grup zasobów. 

Wyniki powinny zawierać następujące kolumny:
- `region` - nazwę regionu
- `how_many` - liczba złamań limitu


## Zadanie 5
Firma w ramach polityki ograniczania wydatków stworzyła system ostrzeżeń i kar za przekraczanie kosztów w ramach pojedynczego naliczenia. Przekroczenie kwoty 200$ generuje ostrzeżenie, a kwota przekroczy 500$, nakładana jest kara.

Wykrywaj przypadki serii składających z co najmniej dwóch ostrzeżeń zakończonych karą w ramach tego samego regionu.
Znalezienie takich serii skutkuje udzieleniem nagany dla regionu. Aby jednak taka nagana została udzielona, seria musi się utworzyć w czasie nie dłuższym niż 30 sekund.

Wyniki powinny zawierać następujące kolumny:
- `start_ets` - data pierwszego ostrzeżenia (naliczenia kosztów skutkującego ostrzeżeniem)
- `cost1` - koszt naliczenia skutkującego pierwszym ostrzeżeniem
- `cost2` - koszt naliczenia skutkującego drugim ostrzeżeniem
- `penalty_ets` - data nałożenia kary (naliczenia kosztów skutkującego nałożeniem kary)
- `penalty_cost` - koszt naliczenia kosztów skutkującego nałożeniem kary
- `region` - nazwa regionu


## Zadanie 6
Firma zwróciła uwagę na dziwny układ trzech kolejnych naliczeń (nie koniecznie następujących bezpośrednio po sobie) dotyczących tej samej grupy zasobów. Układ ten polega on na trzech naliczeniach, każde na kwotę powyżej 200$ z 3 różnych regionów. Pierwsze naliczenie ma miejsce dla regionu 'uk', drugie dla regionu 'germany'. W przypadku trzeciego naliczenia sytuacja dotyczy albo regionu 'asia', albo 'india' (firma nie jest do końca pewna, który z tych dwóch). 

Wykrywaj powyższe przypadki układów pod warunkiem, że w czasie pomiędzy pierwszym naliczeniem a ostatnim nie pojawiło się naliczenie dla tej samej grupy zasobów na kwotę poniżej 10$.

Wyniki powinny zawierać następujące kolumny:
`resourceGroup` - nazwa grupy zasobów, dla której wystąpił wspomniany układ
`ets1` - czas pierwszego naliczenia (dla regionu 'uk') w układzie 
`ets2` - czas drugiego naliczenia (dla regionu 'germany') w układzie 
`ets3` - czas trzeciego naliczenia (dla regionu 'asia' albo 'india') w układzie 
`cost` - suma kosztów ze wszystkich naliczeń w układzie

## Zadanie 7
Firma wynegocjowała rabat dla sytuacji, w których w ramach tej samej grupy zasobów i tego samego regionu pojawia się seria co najmniej trzech naliczeń, w których koszt każdorazowo wzrasta w stosunku do kosztu naliczenia poprzedniego. 
Każda taka seria kończy się oczywiście naliczeniem (z tej samej grupy zasobów i w tym samym regionie), w którym koszt w końcu maleje. 
Rabat polega na anulowaniu kosztu pierwszego naliczenia w takiej serii. 

Wyszukuj wystąpienia powyższych serii. 

Wyniki powinny zawierać następujące kolumny:
 `region` - region zasobów, dla których pojawiła się seria wzrastających kosztów,
 `ets` - czas pierwszego naliczenia w serii, 
 `discount` - wartość rabatu — koszt pierwszego naliczenia w serii, 
 `beforeDisc` - suma kosztów za naliczenia w serii przed odjęciem rabatu,
 `afterDisc` - suma kosztów za naliczenia w serii po odjęciu rabatu,
 `length` - liczba naliczeń ujętych w serii.
