package com.giraone.s3.testdocuments;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

/**
 * Model data class used to generate the PDF content of test documents.
 */
public class TestDocumentContent {

    private String uuid;
    private long timeStamp;
    private long counter;
    private String title;
    private HashMap<String, String> metaData;

    @JsonIgnore
    private String text;

    public TestDocumentContent(long counter, String title, long time, HashMap<String, String> metaData) {
        super();
        this.counter = counter;
        this.uuid = UUID.randomUUID().toString();
        this.timeStamp = time;
        this.title = title;
        this.metaData = metaData;
        this.text = TEXT;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public long getTime() {
        return timeStamp;
    }

    public Date getDate() {
        return new Date(this.timeStamp);
    }

    public void setTime(long time) {
        this.timeStamp = time;
    }

    public long getCounter() {
        return counter;
    }

    public void setCounter(long counter) {
        this.counter = counter;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public HashMap<String, String> getMetaData() {
        return metaData;
    }

    public void setMetaData(HashMap<String, String> metaData) {
        this.metaData = metaData;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    private static final String TEXT =
            "Heute wird Software oft als Dienst geliefert - auch Web App oder Software-As-A-Service genannt. \r\n"
                    + "Die Zwölf-Faktoren-App ist eine Methode um Software-As-A-Service Apps zu bauen die: \r\n"
                    + "deklarative Formate benutzen für die Automatisierung der Konfiguration, um Zeit und Kosten für neue Entwickler im Projekt zu minimieren; \r\n"
                    + "einen sauberen Vertrag mit dem zugrundeliegenden Betriebssystem haben, maximale Portierbarkeit zwischen Ausführungsumgebungen bieten; \r\n"
                    + "sich für das Deployment auf modernen Cloud-Plattformen eignen, die Notwendigkeit von Servern und Serveradministration vermeiden; \r\n"
                    + "die Abweichung minimieren zwischen Entwicklung und Produktion, um Continuous Deployment für maximale Agilität ermöglichen; \r\n"
                    + "und skalieren können ohne wesentliche Änderungen im Tooling, in der Architektur oder in den Entwicklungsverfahren. \r\n"
                    + "\r\n"
                    + "Die Zwölf-Faktoren-Methode kann auf Apps angewendet werden, die in einer beliebigen Programmiersprache geschrieben sind, \r\n"
                    + "und die eine beliebige Kombination von unterstützenden Diensten benutzen (Datenbank, Queue, Cache, …) \r\n"
                    + "Hintergrund \r\n"
                    + "\r\n"
                    + "Die Mitwirkenden an diesem Dokument waren direkt beteiligt an der Entwicklung und dem Deployment von hunderten von \r\n"
                    + "Apps und wurden Zeugen bei der Entwicklung, beim Betrieb und der Skalierung von hunderttausenden von Apps im Rahmen \r\n"
                    + "unserer Arbeit an der Heroku-Plattform. \r\n"
                    + "\r\n"
                    + "Dieses Dokument ist eine Synthese all unserer Erfahrungen und der Beobachtungen einer großen Bandbreite von Software-As-A-Service Apps. \r\n"
                    + "\r\n"
                    + "Es ist eine Bestimmung der idealen Praktiken bei der App-Entwicklung mit besonderem Augenmerk auf die Dynamik des organischen Wachstums \r\n"
                    + "einer App über die Zeit, die Dynamik der Zusammenarbeit zwischen den Entwicklern die an einer Codebase zusammenarbeiten und \r\n"
                    + "der Vermeidung der Kosten von Software-Erosion. \r\n"
                    + "\r\n"
                    + "Unsere Motivation ist, das Bewusstsein zu schärfen für systembedingte Probleme in der aktuellen Applikationsentwicklung, \r\n"
                    + "\r\n"
                    + "ein gemeinsames Vokabular zur Diskussion dieser Probleme zu liefern und ein Lösungsportfolio zu diesen Problemen mit einer \r\n"
                    + "zugehörigen Terminologie anzubieten. Das Format ist angelehnt an Martin Fowlers Bücher Patterns of Enterprise Application Architecture \r\n"
                    + "und Refactoring. \r\n";
}
