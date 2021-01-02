package com.example.springbatchdemo.batch;

import org.springframework.batch.item.ItemProcessor;

import com.example.springbatchdemo.entity.Employee;

public class BatchProcessor implements ItemProcessor<Employee, Employee>{

	@Override
	public Employee process(Employee item) throws Exception {
		//Write business logic to change Employee object
		System.out.println("In processor");
		return item;
	}

}
