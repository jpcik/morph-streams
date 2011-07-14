package es.upm.fi.dia.oeg.sparql.lang.sparqlstream;

import java.io.Reader;
import java.io.StringReader;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.query.QueryParseException;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.shared.JenaException;
import com.hp.hpl.jena.sparql.lang.Parser;
import com.hp.hpl.jena.sparql.lang.arq.ParseException;
import com.hp.hpl.jena.sparql.lang.arq.TokenMgrError;

import es.upm.fi.dia.oeg.sparql.lang.sparqlstream.SPARQLStrParser;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.Template;


public class ParserSPARQLstr extends Parser {


    private interface Action { void exec(SPARQLStrParser parser) throws Exception ; }
    
    @Override
    public Query parse(final Query query, String queryString)
    {
        query.setSyntax(Syntax.syntaxSPARQL) ;

        Action action = new Action() {
            public void exec(SPARQLStrParser parser) throws Exception
            {
                parser.Query();
            }
        } ;

        perform(query, queryString, action) ;
        validateParsedQuery(query) ;
        return query ;
    }
    
    public static Element parseElement(String string)
    {
        final Query query = new Query () ;
        Action action = new Action() {
            public void exec(SPARQLStrParser parser) throws Exception
            {
                Element el = parser.GroupGraphPattern() ;
                query.setQueryPattern(el) ;
            }
        } ;
        perform(query, string, action) ;
        return query.getQueryPattern() ;
    }
    
    public static Template parseTemplate(String string)
    {
        final Query query = new Query () ;
        Action action = new Action() {
            public void exec(SPARQLStrParser parser) throws Exception
            {
                Template t = parser.ConstructTemplate() ;
                query.setConstructTemplate(t) ;
            }
        } ;
        perform(query, string, action) ;
        return query.getConstructTemplate() ;
    }
    
    
    // All throwable handling.
    private static void perform(Query query, String string, Action action)
    {
        Reader in = new StringReader(string) ;
        SPARQLStrParser parser = new SPARQLStrParser(in) ;

        try {
            query.setStrict(true) ;
            parser.setQuery(query) ;
            action.exec(parser) ;
        }
        catch (ParseException ex)
        { 
            throw new QueryParseException(ex.getMessage(),
                                          ex.currentToken.beginLine,
                                          ex.currentToken.beginColumn
                                          ) ; }
        catch (TokenMgrError tErr)
        {
            // Last valid token : not the same as token error message - but this should not happen
            int col = parser.token.endColumn ;
            int line = parser.token.endLine ;
            throw new QueryParseException(tErr.getMessage(), line, col) ; }
        
        catch (QueryException ex) { throw ex ; }
        catch (JenaException ex)  { throw new QueryException(ex.getMessage(), ex) ; }
        catch (Error err)
        {
            // The token stream can throw errors.
            throw new QueryParseException(err.getMessage(), err, -1, -1) ;
        }
        catch (Throwable th)
        {
            //ALog.warn(ParserSPARQLstr.class, "Unexpected throwable: ",th) ;
            throw new QueryException(th.getMessage(), th) ;
        }
    }
}
