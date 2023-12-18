// Fill out your copyright notice in the Description page of Project Settings.

#pragma once

#include "PCGSettings.h"

#include "PCGCopyAttributeElement.generated.h"

/**
 * Copy one or more attributes from another source, by matching an attribute value (instead of by index)
 */
UCLASS()
class PCGPLUS_API UPCGCopyAttributeSettings : public UPCGSettings
{
	GENERATED_BODY()

public:
	//~Begin UPCGSettings interface
#if WITH_EDITOR
	virtual FName GetDefaultNodeName() const override;
	virtual FText GetDefaultNodeTitle() const override;
	virtual EPCGSettingsType GetType() const override { return EPCGSettingsType::Metadata; }
	virtual FText GetNodeTooltipText() const override;
	virtual bool HasDynamicPins() const override { return true; }
#endif
	virtual EPCGDataType GetCurrentPinTypes(const UPCGPin* InPin) const override;
	virtual FName AdditionalTaskName() const override;

protected:
	virtual TArray<FPCGPinProperties> InputPinProperties() const override;
	virtual TArray<FPCGPinProperties> OutputPinProperties() const override;
	virtual FPCGElementPtr CreateElement() const override;
	//~End UPCGSettings interface

public:
	UPROPERTY(BlueprintReadWrite, EditAnywhere, Category = "Settings")
	FPCGAttributePropertyInputSelector SourceAttributeProperty;

	UPROPERTY(BlueprintReadWrite, EditAnywhere, Category = "Settings")
	FPCGAttributePropertyOutputSelector TargetAttributeProperty;

	UPROPERTY(BlueprintReadWrite, EditAnywhere, Category = "Settings")
	bool bMatchByAttribute = true;

	UPROPERTY(BlueprintReadWrite, EditAnywhere, Category = "Settings", meta = (EditCondition = "bMatchByAttribute"))
	FPCGAttributePropertyInputSelector SourceMatchAttributeProperty;

	UPROPERTY(BlueprintReadWrite, EditAnywhere, Category = "Settings", meta = (EditCondition = "bMatchByAttribute"))
	FPCGAttributePropertyInputSelector TargetMatchAttributeProperty;
};

class FPCGCopyAttributeElement : public FSimplePCGElement
{
protected:
	virtual bool ExecuteInternal(FPCGContext* Context) const override;

private:

};
