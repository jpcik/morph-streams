package es.upm.fi.dia.oeg.integration.adapter.gsn;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;

import gsn.webservice.standard.GSNWebServiceStub.GSNWebService_FieldSelector;
import gsn.webservice.standard.GSNWebServiceStub.StandardCriterion;

public class GsnUrlQuery extends GsnQuery 
{
	@Override
	public String serializeQuery()
	{
		String s = "http://planetdata.epfl.ch:22001/multidata?";
		int i=0;
		for (GSNWebService_FieldSelector f:getSelectors())
		{
			s+="&vs["+i+"]="+f.getVsname();
			s+="&field["+i+"]="+StringUtils.join(f.getFieldNames(),',')+"\n";
			i++;
		}
		i=0;
		for (StandardCriterion c:getConditions())
		{
			s+="&c_vs["+i+"]="+c.getVsname();
			s+="&c_filed["+i+"]="+c.getField();
			if (c.getOperator().equals("ge"))
				s+="&c_min["+i+"]="+c.getValue();
			else if (c.getOperator().equals("le"))
				s+="&c_max["+i+"]="+c.getValue();
			else
				throw new NotImplementedException("unknown operator "+c.getOperator());
			s+="&c_join["+i+"]=and\n";
			
		}
		return s;
	}
}
