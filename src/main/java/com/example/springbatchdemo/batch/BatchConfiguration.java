package com.example.springbatchdemo.batch;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.ItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.example.springbatchdemo.entity.Employee;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	 public JobBuilderFactory jobBuilderFactory;
	 
	 @Autowired
	 public StepBuilderFactory stepBuilderFactory;
	 	 
	 @Bean
	 public DataSource dataSource()
	 {
		 final DriverManagerDataSource dataSource = new DriverManagerDataSource();
		  dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
		  dataSource.setUrl("jdbc:mysql://localhost:3306/employee_directory?useSSL=false&serverTimezone=UTC");
		  dataSource.setUsername("springstudent");
		  dataSource.setPassword("springstudent");
		  
		  return dataSource;
	 }
	 
	 @Bean
	 public FlatFileItemReader<Employee> itemReader()
	 {
		 FlatFileItemReader<Employee> flatFileItemReader = new FlatFileItemReader<Employee>();
		 System.out.println("In reader");
		 flatFileItemReader.setResource(new ClassPathResource("employee_data.csv"));
		 flatFileItemReader.setLinesToSkip(1);
		//Configure how each line will be parsed and mapped to different values
		 flatFileItemReader.setLineMapper(new DefaultLineMapper<Employee>() {
		        {
		            //3 columns in each row
		            setLineTokenizer(new DelimitedLineTokenizer() {
		                {
		                    setNames(new String[] { "firstName", "lastName","email" });
		                }
		            });
		            //Set values in Employee class
		            setFieldSetMapper(new BeanWrapperFieldSetMapper<Employee>() {
		                {
		                    setTargetType(Employee.class);
		                }
		            });
		        }
		    });
		 return flatFileItemReader;
	 }
	 
	 @Bean
	 public ItemProcessor<Employee, Employee> itemProcessor()
	 {
		 return new BatchProcessor();
	 }
	 
	 @Bean
	 public JdbcBatchItemWriter<Employee> itemWiter()
	 {
		 System.out.println("In writer");
		 JdbcBatchItemWriter<Employee> jdbcBatchItemWriter = new JdbcBatchItemWriter<Employee>();
		 jdbcBatchItemWriter.setDataSource(dataSource());
		 jdbcBatchItemWriter.setSql("insert into employee(first_name,last_name,email) values(:firstName,:lastName,:email)");
			/*
			 * jdbcBatchItemWriter.setItemPreparedStatementSetter(new
			 * ItemPreparedStatementSetter<Employee>() {
			 * 
			 * @Override public void setValues(Employee item, PreparedStatement ps) throws
			 * SQLException { ps.setString(1, item.getFirstName()); ps.setString(2,
			 * item.getLastName()); ps.setString(3, item.getEmail()); } });
			 */
		 jdbcBatchItemWriter.setItemSqlParameterSourceProvider(new ItemSqlParameterSourceProvider<Employee>() {

			@Override
			public SqlParameterSource createSqlParameterSource(Employee item) {
				return new SqlParameterSource() {
					
					@Override
					public boolean hasValue(String paramName) {
						return true;
					}
					
					@Override
					public Object getValue(String paramName) throws IllegalArgumentException {
						if(paramName.equals("firstName"))
							return item.getFirstName();
						if(paramName.equals("lastName"))
							return item.getLastName();
						if(paramName.equals("email"))
							return item.getEmail();
						return null;
					}
				};
			}
		});
		 return jdbcBatchItemWriter;
	 }
	 
	 @Bean
	 public Step step1() {
	  return stepBuilderFactory.get("step1").<Employee, Employee> chunk(10)
	    .reader(itemReader())
	    .processor(itemProcessor())
	    .writer(itemWiter())
	    .build();
	 }
	 
	 @Bean
	 public Job exportUserJob() {
	  return jobBuilderFactory.get("exportUserJob")
	    .incrementer(new RunIdIncrementer())
	    .flow(step1())
	    .end()
	    .build();
	 }
}
