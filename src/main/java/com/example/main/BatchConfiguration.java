package com.example.main;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.oxm.xstream.XStreamMarshaller;

import com.example.main.model.User;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	
	@Autowired
	public JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	public DataSource dataSource;
	
	@Bean
	public DataSource dataSource() {
		final DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
		dataSource.setUrl("jdbc:mysql://localhost:3306/springbatch");
		dataSource.setUsername("root");
		dataSource.setPassword("root");
		
		return dataSource;
	}
	
	@Bean
	public StaxEventItemReader<User> reader(){
		StaxEventItemReader<User> reader = new StaxEventItemReader<User>();
		reader.setResource(new ClassPathResource("user.xml"));
		reader.setFragmentRootElementName("user");
		
		Map<String, String> aliases =new HashMap<String, String>();
		aliases.put("user", "package com.example.main.model.User");
		
		XStreamMarshaller xStreamMarshaller = new XStreamMarshaller();
		xStreamMarshaller.setAliases(aliases);
		
		reader.setUnmarshaller(xStreamMarshaller);
		
		return reader;
	}
	
	@Bean
	public JdbcBatchItemWriter<User> writer(){
		JdbcBatchItemWriter<User> writer = new JdbcBatchItemWriter<User>();
		writer.setDataSource(dataSource);
		writer.setSql("insert into user(id,name) values (?,?)");
		writer.setItemPreparedStatementSetter(new UserItemPreparedStmSetter());
		
		return writer;
	}
	
	private class UserItemPreparedStmSetter implements ItemPreparedStatementSetter<User> {

		@Override
		public void setValues(User user, PreparedStatement ps) throws SQLException {
			ps.setInt(1, user.getId());
			ps.setString(2, user.getName());
			
		}
		
	}
	
	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1")
				.<User, User> chunk(10)
				.reader(reader())
				.writer(writer())
				.build();
	}
	
	@Bean
	public Job importUserJob() {
		return jobBuilderFactory.get("importUserJob")
				.incrementer(new RunIdIncrementer())
				.flow(step1())
				.end()
				.build();
	}
}
